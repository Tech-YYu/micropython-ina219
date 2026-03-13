from machine import I2C, Pin, RTC
import sys
import select
import struct
import time

CONFIG_32V      = 0x399F   
CONFIG_16V      = 0x199F   
CONFIG_32V_9BIT = 0x3807   
CONFIG_16V_9BIT = 0x1807   

# Active config -- swap these to change speed vs resolution
ACTIVE_32V = CONFIG_32V_9BIT
ACTIVE_16V = CONFIG_16V_9BIT

# ---------------------------------------------------------------
FRAME_MAGIC   = b'\xAA\x55'
FRAME_HDR_FMT = '!IB'         # timestamp_ms(4B) + sensor_count(1B)
SENSOR_FMT    = '!BHH'        # id_status(1B) + voltage(2B) + current(2B)
FRAME_HDR_SZ  = struct.calcsize(FRAME_HDR_FMT)
SENSOR_SZ     = struct.calcsize(SENSOR_FMT)

# Status values -- packed into id_status byte bits[4:3]
STATUS_OK       = 0
STATUS_OVERFLOW = 1
STATUS_ERROR    = 2
STATUS_READFAIL = 3

# Scaling factors (match logger.py exactly)
SCALE_V    = 100    # voltage   x100  -> 0.01V  LSB
SCALE_I    = 1      # current   x1    -> 1mA    LSB

# ---------------------------------------------------------------
# INA219 Driver
# ---------------------------------------------------------------
class INA219:

    # Register addresses
    REG_CONFIG      = 0x00
    REG_SHUNTVOLT   = 0x01
    REG_BUSVOLT     = 0x02
    REG_POWER       = 0x03
    REG_CURRENT     = 0x04
    REG_CALIBRATION = 0x05

    def __init__(self, i2c, addr=0x40, shunt_ohms=0.1, max_amps=3.2, config=CONFIG_32V_9BIT):
        self.i2c        = i2c
        self.addr       = addr
        self.shunt_ohms = shunt_ohms
        self.max_amps   = max_amps
        self.config     = config
        self._calibrate()

    def _write_register(self, reg, value):
        try:
            buf = bytearray(3)
            buf[0] = reg
            buf[1] = (value >> 8) & 0xFF
            buf[2] = value & 0xFF
            self.i2c.writeto(self.addr, buf)
            return True
        except OSError:
            return False

    def _read_register(self, reg):
        try:
            self.i2c.writeto(self.addr, bytearray([reg]), False)
            res = self.i2c.readfrom(self.addr, 2)
            return (res[0] << 8) | res[1]
        except OSError:
            return None

    def _read_signed(self, reg):
        val = self._read_register(reg)
        if val is None:
            return None
        if val > 32767:
            val -= 65536
        return val

    def _calibrate(self):
        """
        Equation 2: Current_LSB = Max_Expected_Current / 2^15
        Equation 1: Cal = trunc(0.04096 / (Current_LSB x R_SHUNT))
        Equation 3: Power_LSB  = 20 x Current_LSB
        """
        self._cal_raw    = int(0.04096 / ((self.max_amps / 32768) * self.shunt_ohms))
        self.current_lsb = self.max_amps / 32768
        self.power_lsb   = self.current_lsb * 20
        self._write_register(self.REG_CONFIG,      self.config)
        self._write_register(self.REG_CALIBRATION, self._cal_raw)

    def recalibrate(self, shunt_ohms, max_amps):
        self.shunt_ohms = shunt_ohms
        self.max_amps   = max_amps
        self._calibrate()

    def reset(self):
        self._write_register(self.REG_CONFIG, 0x8000)
        self._calibrate()

    def bus_voltage_V(self):
        raw = self._read_register(self.REG_BUSVOLT)
        if raw is None:
            return None
        return (raw >> 3) * 0.004

    def shunt_voltage_mV(self):
        raw = self._read_signed(self.REG_SHUNTVOLT)
        if raw is None:
            return None
        return raw * 0.01

    def current_mA(self):
        raw = self._read_signed(self.REG_CURRENT)
        if raw is None:
            return None
        return raw * self.current_lsb * 1000

    def power_mW(self):
        raw = self._read_register(self.REG_POWER)
        if raw is None:
            return None
        return raw * self.power_lsb * 1000

    def supply_voltage_V(self):
        bus   = self.bus_voltage_V()
        shunt = self.shunt_voltage_mV()
        if bus is None or shunt is None:
            return None
        return bus + (shunt / 1000)

    def is_overflow(self):
        raw = self._read_register(self.REG_BUSVOLT)
        if raw is None:
            return False
        return bool(raw & 0x0001)

    def is_connected(self):
        try:
            self.i2c.readfrom(self.addr, 1)
            return True
        except OSError:
            return False


# ---------------------------------------------------------------
# Clock sync
# sync with PC time over USB serial before logging starts.
# ---------------------------------------------------------------
_rtc = RTC()


def sync_clock_with_pc(timeout_ms=15000):
    """
    Send SYNC_REQUEST (ASCII), wait for SYNC,YYYY-MM-DD HH:MM:SS.fff reply.
    Returns True on success.
    """
    print("SYNC_REQUEST")

    poll = select.poll()
    poll.register(sys.stdin, select.POLLIN)
    start = time.ticks_ms()

    while time.ticks_diff(time.ticks_ms(), start) < timeout_ms:
        if not poll.poll(100):
            continue
        line = sys.stdin.readline().strip()
        if not line.startswith("SYNC,"):
            continue
        try:
            ts             = line[5:]
            date_p, time_p = ts.split(" ")
            y, mo, d       = [int(x) for x in date_p.split("-")]
            h, mi, s_ms    = time_p.split(":")
            s_str, ms_str  = s_ms.split(".")
            _rtc.datetime((y, mo, d, 0, int(h), int(mi), int(s_str), int(ms_str) * 1000))
            print("SYNC_OK,{}".format(ts))
            return True
        except Exception as e:
            print("SYNC_ERROR,{}".format(e))
            return False

    print("SYNC_TIMEOUT")
    return False


def measure_latency(samples=10):
    """PING/PONG round-trip latency measurement."""
    poll = select.poll()
    poll.register(sys.stdin, select.POLLIN)
    total_ms   = 0
    successful = 0
    for i in range(samples):
        t_send = time.ticks_ms()
        print("PING,{}".format(i))
        if poll.poll(500):
            line = sys.stdin.readline().strip()
            if line == "PONG,{}".format(i):
                total_ms  += time.ticks_diff(time.ticks_ms(), t_send)
                successful += 1
        time.sleep_ms(50)
    if successful == 0:
        print("LATENCY,0.00ms,samples=0")
        return 0.0
    one_way = (total_ms / successful) / 2
    print("LATENCY,{:.2f}ms,samples={}".format(one_way, successful))
    return one_way


def get_timestamp_ms():
    """
    Return RTC time as milliseconds since midnight (uint32).
    Wraps every 24h (max 86,400,000 ms, well within uint32 limit of 4,294,967,295).
    Used in binary frame for sub-second timestamp precision.
    """
    t  = _rtc.datetime()
    # t = (year, month, day, weekday, hour, min, sec, subsec_us)
    ms = t[4] * 3_600_000 + t[5] * 60_000 + t[6] * 1_000 + t[7] // 1_000
    return ms & 0xFFFFFFFF


def get_timestamp_str():
    """Return RTC time as ISO string for console output."""
    t  = _rtc.datetime()
    ms = t[7] // 1000
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}".format(
        t[0], t[1], t[2], t[4], t[5], t[6], ms)


# ---------------------------------------------------------------
# Binary frame builder
# ---------------------------------------------------------------
def crc8(data):
    """XOR CRC8 -- fast on MicroPython, sufficient for framing."""
    crc = 0
    for b in data:
        crc ^= b
    return crc


def build_frame(readings):
    """
    Build a 48-byte binary frame from a list of sensor readings.

    readings: list of dicts with keys:
        sensor_id, voltage_V, current_mA, status
        (power_mW derived by logger as V*I; shunt/supply dropped)

    Returns: bytes ready to write to serial.
    """
    ts_ms  = get_timestamp_ms()
    n      = len(readings)
    header = struct.pack(FRAME_HDR_FMT, ts_ms, n)

    body = bytearray(n * SENSOR_SZ)
    off  = 0
    for r in readings:
        id_status = (r['sensor_id'] << 5) | (r['status'] << 3)
        v = max(0, min(65535, int(round(r['voltage_V']  * SCALE_V))))
        i = max(0, min(65535, int(round(r['current_mA'] * SCALE_I))))
        struct.pack_into(SENSOR_FMT, body, off, id_status, v, i)
        off += SENSOR_SZ

    payload = header + bytes(body)
    crc     = crc8(payload)
    return FRAME_MAGIC + payload + bytes([crc])


# ---------------------------------------------------------------
# Sensor configuration
#
# I2C address table -- INA219 Datasheet Table 1 (section 8.5.5.1)
#   A1    A0    Address
#   GND   GND   0x40
#   GND   VS+   0x41
#   GND   SDA   0x42
#   GND   SCL   0x43
#   VS+   GND   0x44
#   VS+   VS+   0x45
#   VS+   SDA   0x46
#   VS+   SCL   0x47
# ---------------------------------------------------------------
SENSORS = [
    # ===========================================================
    # MAIN BOARD -- hardcoded addresses
    # ===========================================================
    {
        "chip": "U3", "name": "ATX12", "location": "main",
        "addr": 0x40, "shunt_ohms": 0.033, "max_amps": 15.0,
        "config": ACTIVE_32V,
    },
    {
        "chip": "U4", "name": "ATX5", "location": "main",
        "addr": 0x41, "shunt_ohms": 0.033, "max_amps": 10.0,
        "config": ACTIVE_16V,
    },
    {
        "chip": "U5", "name": "BO12", "location": "main",
        "addr": 0x42, "shunt_ohms": 0.033, "max_amps": 15.0,
        "config": ACTIVE_32V,
    },
    {
        "chip": "U6", "name": "BO3.3", "location": "main",
        "addr": 0x43, "shunt_ohms": 0.033, "max_amps": 10.0,
        "config": ACTIVE_16V,
    },
    # ===========================================================
    # SUB-BOARD -- set jumpers BEFORE powering on
    # ===========================================================
    {
        "chip": "U1", "name": "PCIe_1", "location": "subboard",
        "addr": 0x44, "shunt_ohms": 0.033, "max_amps": 10.0,
        "config": ACTIVE_32V,
    },
    {
        "chip": "U2", "name": "PCIe_2", "location": "subboard",
        "addr": 0x45, "shunt_ohms": 0.033, "max_amps": 10.0,
        "config": ACTIVE_32V,
    },
    {
        "chip": "U3", "name": "EPS_1", "location": "subboard",
        "addr": 0x46, "shunt_ohms": 0.033, "max_amps": 20.0,
        "config": ACTIVE_32V,
    },
    {
        "chip": "U4", "name": "EPS_2", "location": "subboard",
        "addr": 0x47, "shunt_ohms": 0.033, "max_amps": 20.0,
        "config": ACTIVE_32V,
    },
]


# ---------------------------------------------------------------

VOLTAGE_RANGES = {
    "ATX12":  (11.40, 12.60),
    "ATX5":   ( 4.75,  5.25),
    "BO12":   (11.40, 12.60),
    "BO3.3":  ( 3.135, 3.465),
    "PCIe_1": (11.40, 12.60),
    "PCIe_2": (11.40, 12.60),
    "EPS_1":  (11.40, 12.60),
    "EPS_2":  (11.40, 12.60),
}

JUMPER_HINTS = {
    0x40: "A1=GND  A0=GND  (hardwired on main board)",
    0x41: "A1=GND  A0=VS+  (hardwired on main board)",
    0x42: "A1=GND  A0=SDA  (hardwired on main board)",
    0x43: "A1=GND  A0=SCL  (hardwired on main board)",
    0x44: "A1=VS+  A0=GND  (jumper on sub-board)",
    0x45: "A1=VS+  A0=VS+  (jumper on sub-board)",
    0x46: "A1=VS+  A0=SDA  (jumper on sub-board)",
    0x47: "A1=VS+  A0=SCL  (jumper on sub-board)",
}


# ---------------------------------------------------------------
# Startup
# ---------------------------------------------------------------
i2c = I2C(0, scl=Pin(1), sda=Pin(0), freq=1_000_000)

print("=" * 55)
print("INA219 Power Monitor -- Startup (500 Hz binary mode)")
print("=" * 55)

# Step 1: Sync clock with PC
synced = sync_clock_with_pc(timeout_ms=15000)
if not synced:
    print("WARNING: Running without time sync -- timestamps from epoch")

# Step 2: Measure serial latency
measure_latency(samples=10)

# Step 3: Scan I2C bus and initialise sensors
print("Scanning I2C bus...")
found = i2c.scan()
print("Found {} device(s): {}".format(len(found), [hex(a) for a in found]))
print()

# Step 3b: Initialise sensors -- skip anything not on bus
sensors = []
print("{:<6} {:<10} {:<10} {:<6} {:<8} {}".format(
    "Chip", "Name", "Location", "Addr", "Config", "Status"))
print("-" * 55)

for idx, cfg in enumerate(SENSORS):
    config_val = cfg["config"]
    config_str = "9bit-32V" if config_val == CONFIG_32V_9BIT else \
                 "9bit-16V" if config_val == CONFIG_16V_9BIT else \
                 "12bit-32V" if config_val == CONFIG_32V else "12bit-16V"

    if cfg["addr"] in found:
        try:
            sensor = INA219(
                i2c,
                addr       = cfg["addr"],
                shunt_ohms = cfg["shunt_ohms"],
                max_amps   = cfg["max_amps"],
                config     = config_val,
            )
            sensors.append((idx, cfg, sensor))
            print("{:<6} {:<10} {:<10} {:<6} {:<8} OK".format(
                cfg["chip"], cfg["name"], cfg["location"],
                hex(cfg["addr"]), config_str))
        except Exception as e:
            print("{:<6} {:<10} {:<10} {:<6} {:<8} INIT ERROR: {}".format(
                cfg["chip"], cfg["name"], cfg["location"],
                hex(cfg["addr"]), config_str, e))
    else:
        print("{:<6} {:<10} {:<10} {:<6} {:<8} NOT FOUND".format(
            cfg["chip"], cfg["name"], cfg["location"],
            hex(cfg["addr"]), config_str))

print()
print("{}/{} sensor(s) ready.".format(len(sensors), len(SENSORS)))

# Step 4: Missing sensor warning
missing = [cfg for cfg in SENSORS if cfg["addr"] not in found]
if missing:
    print()
    print("!" * 55)
    print("WARNING: {}/{} sensor(s) not found on I2C bus:".format(
        len(missing), len(SENSORS)))
    print()
    for cfg in missing:
        hint = JUMPER_HINTS.get(cfg["addr"], "check wiring")
        print("  {} {} @ {}  -->  {}".format(
            cfg["chip"], cfg["name"], hex(cfg["addr"]), hint))
    print()
    print("Possible causes:")
    print("  - Sub-board not connected")
    print("  - Jumper set to wrong position")
    print("  - Rail not powered")
    print()
    print("Auto-continuing with {}/{} sensors in 10s...".format(
        len(sensors), len(SENSORS)))
    print("!" * 55)
    time.sleep(10)
else:
    print("All sensors found. Proceeding.")

print("=" * 55)
print()

# Step 5: Voltage sanity check
print("Running voltage sanity check (ATX spec +-5%)...")
mapping_warnings = 0

for idx, cfg, sensor in sensors:
    name = cfg["name"]
    if name not in VOLTAGE_RANGES:
        continue
    v_min, v_max = VOLTAGE_RANGES[name]
    try:
        v = sensor.bus_voltage_V()
        if v is None:
            print("  {} @ {}  -- could not read voltage".format(
                name, hex(cfg["addr"])))
            continue
        if v_min <= v <= v_max:
            print("  {} @ {}  {:.3f}V  OK  (expected {:.3f}-{:.3f}V)".format(
                name, hex(cfg["addr"]), v, v_min, v_max))
        else:
            mapping_warnings += 1
            print("  {} @ {}  {:.3f}V  MAPPING WARNING".format(
                name, hex(cfg["addr"]), v))
            print("    Expected {:.3f}-{:.3f}V -- possible wrong jumper".format(
                v_min, v_max))
            for other_name, (lo, hi) in VOLTAGE_RANGES.items():
                if other_name != name and lo <= v <= hi:
                    print("    Voltage matches expected range for '{}'".format(
                        other_name))
                    break
    except OSError as e:
        print("  {} @ {}  READ ERROR: {}".format(name, hex(cfg["addr"]), e))

if mapping_warnings == 0:
    print("Voltage sanity check passed.")
else:
    print()
    print("!" * 55)
    print("WARNING: {} sensor(s) outside expected voltage range.".format(
        mapping_warnings))
    print("Check jumper positions against address table before")
    print("trusting logged data.")
    print("!" * 55)

print()

# ---------------------------------------------------------------
# Logger must switch to binary parser when it sees this line.
# ---------------------------------------------------------------
print("DATA_START")
sys.stdout.flush()

# ---------------------------------------------------------------
# Main loop -- 48-byte binary frames, no sleep
# I2C reads per sensor (BUSVOLT, CURRENT) at 1MHz
# ---------------------------------------------------------------
_stdout_write = sys.stdout.buffer.write   # direct buffer write, faster than print

while True:
    readings = []

    for idx, cfg, sensor in sensors:
        try:
            # Read BUSVOLT once -- extract overflow flag AND voltage
            raw_bus = sensor._read_register(sensor.REG_BUSVOLT)
            if raw_bus is None:
                readings.append({
                    'sensor_id':  idx,
                    'voltage_V':  0.0,
                    'current_mA': 0.0,
                    'status':     STATUS_READFAIL,
                })
                continue

            overflow  = bool(raw_bus & 0x0001)
            voltage_V = (raw_bus >> 3) * 0.004

            if overflow:
                readings.append({
                    'sensor_id':  idx,
                    'voltage_V':  voltage_V,
                    'current_mA': 0.0,
                    'status':     STATUS_OVERFLOW,
                })
                continue

            # Read CURRENT register -- calibrated mA value
            raw_i = sensor._read_signed(sensor.REG_CURRENT)
            if raw_i is None:
                readings.append({
                    'sensor_id':  idx,
                    'voltage_V':  voltage_V,
                    'current_mA': 0.0,
                    'status':     STATUS_ERROR,
                })
                continue

            readings.append({
                'sensor_id':  idx,
                'voltage_V':  voltage_V,
                'current_mA': raw_i * sensor.current_lsb * 1000,
                'status':     STATUS_OK,
            })

        except OSError:
            readings.append({
                'sensor_id':  idx,
                'voltage_V':  0.0,
                'current_mA': 0.0,
                'status':     STATUS_READFAIL,
            })

    _stdout_write(build_frame(readings))