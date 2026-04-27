from machine import I2C, Pin, RTC
import sys
import select
import struct
import time

# ── ADC config options ────────────────────────────────────────────────────────
CONFIG_32V      = 0x399F   # 12-bit, 32V range — general 12V rails
CONFIG_16V      = 0x199F   # 12-bit, 16V range — 3.3V / 5V rails (better res)
CONFIG_32V_9BIT = 0x3807   # 9-bit,  32V range — fastest, lowest res
CONFIG_16V_9BIT = 0x1807   # 9-bit,  16V range — fastest on low-V rails

CONFIG_LABELS = {
    CONFIG_32V:      "12bit-32V",
    CONFIG_16V:      "12bit-16V",
    CONFIG_32V_9BIT: " 9bit-32V",
    CONFIG_16V_9BIT: " 9bit-16V",
}

# Speed/precision tradeoff:
# - True: use 9-bit ADC for highest sample rate
# - False: use 12-bit ADC for lower noise / higher precision
HIGH_SPEED_MODE = True
DEFAULT_12V_CONFIG = CONFIG_32V_9BIT if HIGH_SPEED_MODE else CONFIG_32V
DEFAULT_3V3_CONFIG = CONFIG_16V_9BIT if HIGH_SPEED_MODE else CONFIG_16V

# I2C clock (Hz). Tested values: 1_000_000, 1_500_000, 2_000_000
I2C_FREQ_HZ = 1_000_000
# ── Binary frame protocol ─────────────────────────────────────────────────────
FRAME_MAGIC   = b'\xAA\x55'
FRAME_HDR_FMT = '!IB'        # timestamp_ms(4B) + sensor_count(1B)
SENSOR_FMT    = '!BHH'       # id_status(1B) + voltage(2B) + current(2B)
FRAME_HDR_SZ  = struct.calcsize(FRAME_HDR_FMT)
SENSOR_SZ     = struct.calcsize(SENSOR_FMT)

# Status values packed into id_status byte bits [4:3]
STATUS_OK       = 0
STATUS_OVERFLOW = 1
STATUS_ERROR    = 2
STATUS_READFAIL = 3

# Scaling factors — must match logger.py exactly
SCALE_V = 100   # voltage ×100 → 0.01V LSB
SCALE_I = 1     # current ×1   → 1mA  LSB

# ── Display constants ─────────────────────────────────────────────────────────
COL_WIDTH = 72
NAME_W    = 10

# ── Result constants (voltage sanity) ────────────────────────────────────────
_PASS      = "PASS"
_UNDERVOLT = "UNDERVOLT"
_OVERVOLT  = "OVERVOLT"
_UNPOWERED = "UNPOWERED"
_NO_SENSOR = "NO SENSOR"

# ── Shunt resistance ──────────────────────────────────────────────────────────
_R = 0.010 / 3   # 3 × 10mΩ in parallel = 3.333mΩ — default for mainboards / db1 / db2

# ── Board selector + calibration ──────────────────────────────────────────────
# Set BOARD to match the PCB set currently connected.
# "mainboard1_db1" = mainboard1 (U1–U4 cal) + daughterboard1 (U7 cal)
# "mainboard2_db2" = mainboard2 (U2–U4 cal) + daughterboard2 (U7 cal)
# "mainboard2_db3" = mainboard2 (U2–U4 cal) + daughterboard3 U8 EPS-D (1.667mΩ, cal)
BOARD = "mainboard2_db3"

# Linear calibration: cal_mA = m * shunt_calc_mA + c
# Derived from regression of (shunt_calc_avg, psu_ref) pairs at 2A–7A.
_CAL = {
    "mainboard1_db1": {
        0x44: (0.982647, +2.554810),   # U1 PCIe   — mainboard1  — gain −1.74%, R²=0.99999794
        0x45: (0.986081, -2.887387),   # U2 EPS    — mainboard1  — gain −1.39%, R²=0.99999552
        0x40: (0.978480, +18.088053),  # U3 ATX24  — mainboard1  — gain −2.15%, R²=0.99999831
        0x41: (0.994885, +6.354189),   # U4 ATX24  — mainboard1  — gain −0.51%, R²=0.99999848
        0x42: (1.000000, +0.000000),   # U5 BO12   — UNCALIBRATED
        0x43: (1.000000, +0.000000),   # U6 BO3.3  — UNCALIBRATED
        0x4A: (0.985866, +15.048650),  # U7 EPS-D  — daughterboard1 — gain −1.41%, R²=0.99999779
    },
    "mainboard2_db2": {
        0x44: (1.000000, +0.000000),   # U1 PCIe   — UNCALIBRATED
        0x45: (0.988320, +4.674925),   # U2 EPS    — mainboard2  — gain −1.17%, R²=0.99999651
        0x40: (0.978309, +13.540334),  # U3 ATX24  — mainboard2  — gain −2.17%, R²=0.99999947
        0x41: (0.994142, +0.017419),   # U4 ATX24  — mainboard2  — gain −0.59%, R²=0.99999865
        0x42: (1.000000, +0.000000),   # U5 BO12   — UNCALIBRATED
        0x43: (1.000000, +0.000000),   # U6 BO3.3  — UNCALIBRATED
        0x4A: (0.991935, +6.383884),   # U7 EPS-D  — daughterboard2 — gain −0.81%, R²=0.99999841
    },
    "mainboard2_db3": {
        0x44: (1.000000, +0.000000),   # U1 PCIe   — UNCALIBRATED
        0x45: (0.988320, +4.674925),   # U2 EPS    — mainboard2     (same chip as db2)
        0x40: (0.978309, +13.540334),  # U3 ATX24  — mainboard2     (same chip as db2)
        0x41: (0.994142, +0.017419),   # U4 ATX24  — mainboard2     (same chip as db2)
        0x42: (1.000000, +0.000000),   # U5 BO12   — UNCALIBRATED
        0x43: (1.000000, +0.000000),   # U6 BO3.3  — UNCALIBRATED
        0x4A: (0.983139, +13.540789),  # U8 EPS-D  — daughterboard3 — gain −1.69%, R²=0.99999765 (2026-04-22)
    },
}
CAL = _CAL[BOARD]

# ── Sensor table ──────────────────────────────────────────────────────────────
# Format: (stable_id, addr, chip, name, location, shunt_ohms, max_amps,
#           config, v_min, v_max)



SENSORS = [
    # id  addr   chip   name      location     shunt  max_a   config      v_min   v_max
    # (0,  0x44, "U1", "PCIe",   "pcie_eps",  _R,  9.9,  DEFAULT_12V_CONFIG,  11.40, 12.60),
    # (1,  0x45, "U2", "EPS",    "pcie_eps",  _R, 18.0,  DEFAULT_12V_CONFIG,  11.40, 12.60),
    # (2,  0x40, "U3", "ATX24",  "atx24",     _R, 15.0,  DEFAULT_12V_CONFIG,  11.40, 12.60),
    # (3,  0x41, "U4", "ATX24",  "atx24",     _R, 10.0,  DEFAULT_12V_CONFIG,  11.40, 12.60),
    # (4,  0x42, "U5", "BO12",   "atx24",     _R, 15.0,  DEFAULT_12V_CONFIG,  11.40, 12.60),
    # (5,  0x43, "U6", "BO3.3",  "atx24",     _R, 10.0,  DEFAULT_3V3_CONFIG,   3.135, 3.465),
    (6,  0x4A, "U7", "EPS-D",  "daughter",  _R, 32.0,  DEFAULT_12V_CONFIG,  11.40, 12.60),
]

# ── Per-board physical overrides ──────────────────────────────────────────────
# Rewrites fields of SENSORS rows when the selected BOARD uses a physical
# variant at a given address (e.g. daughterboard3 has 1.667mΩ, labeled U8).
_BOARD_OVERRIDES = {
    "mainboard2_db3": {
        # 3 × 5mΩ parallel = 1.667mΩ (half the thermal dissipation of db1/db2).
        # max_a raised to 96A so the INA219 OVF flag doesn't clip A100 transient
        # peaks (observed ~53A; SXM4 theoretical transient ceiling ~67A). At
        # 1.667mΩ, 96A is still well under the PGA limit (~192A) and resolution
        # loss (2.93 mA/LSB) is below the frame's 1 mA quantisation.
        0x4A: {"chip": "U8", "shunt": 0.005 / 3, "max_a": 96.0},
    },
}
_ov = _BOARD_OVERRIDES.get(BOARD, {})
if _ov:
    _new = []
    for sid, addr, chip, name, location, shunt, max_a, config, v_min, v_max in SENSORS:
        o = _ov.get(addr, {})
        _new.append((
            sid, addr,
            o.get("chip",  chip),
            o.get("name",  name),
            o.get("location", location),
            o.get("shunt", shunt),
            o.get("max_a", max_a),
            o.get("config", config),
            o.get("v_min", v_min),
            o.get("v_max", v_max),
        ))
    SENSORS = _new

# ── Startup assertion: sensor_id must fit in 3-bit frame field (max 7) ────────
for _row in SENSORS:
    assert _row[0] < 8, \
        "sensor_id {} ('{}') exceeds 3-bit frame field — max 7".format(
            _row[0], _row[3])

# Consecutive I2C failure threshold before reconfiguration is attempted
MAX_CONSECUTIVE_FAILS = 50

# Jumper hints — shown in startup warning if sensor not found
JUMPER_HINTS = {
    0x44: "U1 PCIe  A1=VS+ A0=GND  (jumper: verify A0 pad shorted to GND)",
    0x45: "U2 EPS   A1=VS+ A0=VS+  (jumper: verify A0 pad shorted to VS+)",
    0x40: "U3 ATX24 A1=GND A0=GND  (hardwired: check solder/component)",
    0x41: "U4 ATX24 A1=GND A0=VS+  (hardwired: check solder/component)",
    0x42: "U5 BO12  A1=GND A0=SDA  (hardwired: check solder/component)",
    0x43: "U6 BO3.3 A1=GND A0=SCL  (hardwired: check solder/component)",
    0x4A: "U7/U8 EPS-D A1=SDA A0=SDA  (daughterboard: check daughterboard connected)",
}

# ── INA219 driver ─────────────────────────────────────────────────────────────

class INA219:

    REG_CONFIG      = 0x00
    REG_SHUNTVOLT   = 0x01
    REG_BUSVOLT     = 0x02
    REG_POWER       = 0x03
    REG_CURRENT     = 0x04
    REG_CALIBRATION = 0x05

    def __init__(self, i2c, addr, shunt_ohms, max_amps, config):
        self.i2c        = i2c
        self.addr       = addr
        self.shunt_ohms = shunt_ohms
        self.max_amps   = max_amps
        self.config     = config
        self._ok        = self._calibrate()   # False if I2C absent at boot

    def _write_reg(self, reg, value):
        try:
            buf    = bytearray(3)
            buf[0] = reg
            buf[1] = (value >> 8) & 0xFF
            buf[2] =  value       & 0xFF
            self.i2c.writeto(self.addr, buf)
            return True
        except OSError:
            return False

    def _read_reg_u(self, reg):
        """Unsigned 16-bit register read."""
        try:
            self.i2c.writeto(self.addr, bytearray([reg]), False)
            d = self.i2c.readfrom(self.addr, 2)
            return (d[0] << 8) | d[1]
        except OSError:
            return None

    def _read_reg_s(self, reg):
        """Signed 16-bit register read (two's complement)."""
        v = self._read_reg_u(reg)
        if v is None:
            return None
        return v - 65536 if v > 32767 else v

    def _calibrate(self):
        """Write config and calibration registers. Returns True on success."""
        self._cal_raw    = int(0.04096 / ((self.max_amps / 32768) * self.shunt_ohms))
        self.current_lsb = self.max_amps / 32768
        # power_lsb is mW/bit — do NOT multiply by 1000 in power_mW()
        self.power_lsb   = self.current_lsb * 20
        ok  = self._write_reg(self.REG_CONFIG,      self.config)
        ok &= self._write_reg(self.REG_CALIBRATION, self._cal_raw)
        return ok

    def recalibrate(self, shunt_ohms, max_amps):
        self.shunt_ohms = shunt_ohms
        self.max_amps   = max_amps
        self._ok        = self._calibrate()

    def reset(self):
        """Hardware reset. Waits 1ms for RST bit to self-clear before reconfiguring."""
        self._write_reg(self.REG_CONFIG, 0x8000)
        time.sleep_ms(1)   # datasheet §8.6.1: RST self-clears after ~30µs
        self._ok = self._calibrate()

    def is_connected(self):
        """Probe by reading config register — avoids stale-data side effects."""
        return self._read_reg_u(self.REG_CONFIG) is not None

    def is_overflow(self):
        raw = self._read_reg_u(self.REG_BUSVOLT)
        if raw is None:
            return False
        return bool(raw & 0x0001)   # OVF flag is bit 0

    def bus_voltage_V(self):
        raw = self._read_reg_u(self.REG_BUSVOLT)
        if raw is None:
            return None
        # Mask lower 3 bits (CNVR/OVF flags) before shifting
        return ((raw & 0xFFF8) >> 3) * 0.004

    def shunt_voltage_mV(self):
        raw = self._read_reg_s(self.REG_SHUNTVOLT)
        if raw is None:
            return None
        return raw * 0.01   # LSB = 10µV → mV

    def current_mA(self):
        raw = self._read_reg_s(self.REG_CURRENT)
        if raw is None:
            return None
        return raw * self.current_lsb * 1000

    def power_mW(self):
        raw = self._read_reg_u(self.REG_POWER)
        if raw is None:
            return None
        # power_lsb already in mW/bit — fixed: original multiplied by 1000 (gave µW)
        return raw * self.power_lsb

    def supply_voltage_V(self):
        """Bus voltage + shunt drop = true supply voltage."""
        bus   = self.bus_voltage_V()
        shunt = self.shunt_voltage_mV()
        if bus is None or shunt is None:
            return None
        return bus + (shunt / 1000.0)

# ── Clock sync ────────────────────────────────────────────────────────────────

_rtc = RTC()

def sync_clock_with_pc(timeout_ms=15000):
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
            # RTC subseconds field is microseconds — multiply ms by 1000
            # Fixed: original stored ms directly, timestamps were off by 1000×
            _rtc.datetime((y, mo, d, 0, int(h), int(mi), int(s_str),
                           int(ms_str) * 1000))
            print("SYNC_OK,{}".format(ts))
            return True
        except Exception as e:
            print("SYNC_ERROR,{}".format(e))
            return False

    print("SYNC_TIMEOUT")
    return False

def measure_latency(samples=10):
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
    # On this Pico 2W MicroPython build, RTC.datetime()[7] (subseconds) stays
    # stuck at 0, so RTC-derived ms quantises to whole-second boundaries —
    # breaks ms-resolution logging (verified empirically). time.ticks_ms()
    # ticks correctly and matches pclogger's semantics (pico_ms = monotonic,
    # use for intervals). Rollover at 2^30 ms (~12.4 days) is handled host-side
    # by pclogger's PICO_TICKS_MODULUS_MS.
    return time.ticks_ms() & 0xFFFFFFFF

def get_timestamp_str():
    t  = _rtc.datetime()
    ms = t[7] // 1000   # µs → ms
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}".format(
        t[0], t[1], t[2], t[4], t[5], t[6], ms)

# ── Binary frame builder ──────────────────────────────────────────────────────

def crc8(data):
    """XOR checksum — must match logger.py exactly."""
    crc = 0
    for b in data:
        crc ^= b
    return crc

def build_frame(readings):
    ts_ms  = get_timestamp_ms()
    n      = len(readings)
    header = struct.pack(FRAME_HDR_FMT, ts_ms, n)

    body = bytearray(n * SENSOR_SZ)
    off  = 0
    for r in readings:
        # stable_id in bits [7:5] (3 bits, max 7) — validated at startup
        id_status = (r['sensor_id'] << 5) | (r['status'] << 3)
        v = max(0, min(65535, int(round(r['voltage_V']  * SCALE_V))))
        i = max(0, min(65535, int(round(r['current_mA'] * SCALE_I))))
        struct.pack_into(SENSOR_FMT, body, off, id_status, v, i)
        off += SENSOR_SZ

    payload = header + bytes(body)
    return FRAME_MAGIC + payload + bytes([crc8(payload)])

# ── I2C bus + sensor init ─────────────────────────────────────────────────────

# Pico 2W: I2C0 on GP0(SDA)/GP1(SCL)
i2c = I2C(0, scl=Pin(1), sda=Pin(0), freq=I2C_FREQ_HZ)

print("=" * COL_WIDTH)
print("INA219 Power Monitor -- Startup")
print("PCB: ATX24 + PCIe/EPS sub-board | MCU: Pico 2W")
print("Board: {}".format(BOARD))
print("=" * COL_WIDTH)

# Step 1: Clock sync
synced = sync_clock_with_pc(timeout_ms=15000)
if not synced:
    print("WARNING: Running without time sync -- timestamps from epoch")

# Step 2: Serial latency
measure_latency(samples=10)

# Step 3: I2C scan + sensor init
print("\nScanning I2C bus...")
found = i2c.scan()
print("Found {} device(s): {}".format(
    len(found), ["0x{:02X}".format(a) for a in found]))
print()

sensors    = []   # list of (stable_id, cfg_tuple, INA219)
sensor_ok  = {}   # addr -> bool

print("{:<6} {:<{nw}} {:<10} {:<6} {:<10} {}".format(
    "Chip", "Name", "Location", "Addr", "Config", "Status", nw=NAME_W))
print("-" * COL_WIDTH)

for row in SENSORS:
    sid, addr, chip, name, location, shunt, max_a, config, v_min, v_max = row
    config_str = CONFIG_LABELS.get(config, "unknown")
    addr_str   = "0x{:02X}".format(addr)

    if addr in found:
        try:
            sensor = INA219(i2c, addr, shunt, max_a, config)
            if sensor._ok:
                sensors.append((sid, row, sensor))
                sensor_ok[addr] = True
                print("{:<6} {:<{nw}} {:<10} {:<6} {:<10} OK".format(
                    chip, name, location, addr_str, config_str, nw=NAME_W))
            else:
                sensor_ok[addr] = False
                print("{:<6} {:<{nw}} {:<10} {:<6} {:<10} INIT FAILED".format(
                    chip, name, location, addr_str, config_str, nw=NAME_W))
        except Exception as e:
            sensor_ok[addr] = False
            print("{:<6} {:<{nw}} {:<10} {:<6} {:<10} ERROR: {}".format(
                chip, name, location, addr_str, config_str, e, nw=NAME_W))
    else:
        sensor_ok[addr] = False
        print("{:<6} {:<{nw}} {:<10} {:<6} {:<10} NOT FOUND".format(
            chip, name, location, addr_str, config_str, nw=NAME_W))

print()
print("{}/{} sensor(s) ready.".format(len(sensors), len(SENSORS)))

# Precompute per-sensor constants used in the hot path.
# calc_mA = (shunt_mV / shunt_ohms) ; then cal_mA = m * calc_mA + c
active_channels = []
for sid, row, sensor in sensors:
    _, addr, _, _, _, shunt, _, _, _, _ = row
    cal_m, cal_c = CAL.get(addr, (1.0, 0.0))
    active_channels.append((sid, sensor, 1.0 / shunt, cal_m, cal_c))

# Step 4: Missing sensor warnings
missing = [row for row in SENSORS if not sensor_ok.get(row[1], False)]
if missing:
    print()
    print("!" * COL_WIDTH)
    print("WARNING: {}/{} sensor(s) not found:".format(len(missing), len(SENSORS)))
    print()
    for row in missing:
        sid, addr, chip, name, *_ = row
        hint = JUMPER_HINTS.get(addr, "check wiring")
        print("  {} {} @ 0x{:02X}  -->  {}".format(chip, name, addr, hint))
    print()
    print("Possible causes:")
    print("  - PCIe/EPS sub-board not connected")
    print("  - Daughterboard not connected (U7 EPS-D @ 0x4A)")
    print("  - Jumper set to wrong position (see ADDR CONFIG on silkscreen)")
    print("  - Rail not powered")
    print("  - CAUTION: Do NOT swap PCIe/EPS cables (different pinouts!)")
    print()
    print("Auto-continuing with {}/{} sensors in 10s...".format(
        len(sensors), len(SENSORS)))
    print("!" * COL_WIDTH)
    time.sleep(10)
else:
    print("All sensors found. Proceeding.")

# Step 5: Voltage sanity check
print()
print("=" * COL_WIDTH)
print("Voltage sanity check (ATX spec ±5%)...")
print("{:<6} {:<{nw}} {:<6} {:<8} {:<8} {:<14} {}".format(
    "Chip", "Name", "Addr", "Bus V", "Supply V", "Expected", "Result",
    nw=NAME_W))
print("-" * COL_WIDTH)

mapping_warnings = 0
counts = {_PASS: 0, _UNDERVOLT: 0, _OVERVOLT: 0, _UNPOWERED: 0, _NO_SENSOR: 0}

for sid, row, sensor in sensors:
    sid, addr, chip, name, location, shunt, max_a, config, v_min, v_max = row
    addr_str = "0x{:02X}".format(addr)
    expected = "{:.3f}-{:.3f}V".format(v_min, v_max)

    try:
        bus_raw = sensor._read_reg_u(sensor.REG_BUSVOLT)
        if bus_raw is None:
            counts[_NO_SENSOR] += 1
            print("{:<6} {:<{nw}} {:<6} {:>8} {:>8} {:<14} {}".format(
                chip, name, addr_str, "--", "--", expected, _NO_SENSOR,
                nw=NAME_W))
            continue

        if bus_raw & 0x0001:
            counts[_NO_SENSOR] += 1
            print("{:<6} {:<{nw}} {:<6} {:>8} {:>8} {:<14} ADC OVERFLOW".format(
                chip, name, addr_str, "--", "--", expected, nw=NAME_W))
            continue

        bus_v  = ((bus_raw & 0xFFF8) >> 3) * 0.004
        shnt_v = sensor.shunt_voltage_mV()
        sup_v  = bus_v + (shnt_v / 1000.0) if shnt_v is not None else bus_v

        if sup_v < 0.5:
            result = _UNPOWERED
        elif sup_v < v_min:
            result = _UNDERVOLT
            mapping_warnings += 1
        elif sup_v > v_max:
            result = _OVERVOLT
            mapping_warnings += 1
        else:
            result = _PASS

        counts[result] += 1
        print("{:<6} {:<{nw}} {:<6} {:>7.3f}V {:>7.3f}V {:<14} {}".format(
            chip, name, addr_str, bus_v, sup_v, expected, result, nw=NAME_W))

        # Cross-check: does voltage match a different sensor's expected range?
        if result in (_UNDERVOLT, _OVERVOLT):
            for other in SENSORS:
                o_name, o_vmin, o_vmax = other[3], other[8], other[9]
                if o_name != name and o_vmin <= sup_v <= o_vmax:
                    print("    ^ voltage matches '{}' range"
                          " -- possible wrong jumper".format(o_name))
                    break

    except OSError as e:
        counts[_NO_SENSOR] += 1
        print("{:<6} {:<{nw}} {:<6} {:>8} {:>8} {:<14} READ ERROR: {}".format(
            chip, name, addr_str, "--", "--", expected, e, nw=NAME_W))

print("-" * COL_WIDTH)
parts   = ["{} {}".format(v, k) for k, v in counts.items() if v]
overall = ("ALL PASS"
           if mapping_warnings == 0 and counts[_NO_SENSOR] == 0
           else "CHECK ABOVE")
print("=> {}  ({})".format(overall, ", ".join(parts)))

if mapping_warnings:
    print()
    print("!" * COL_WIDTH)
    print("WARNING: {} sensor(s) outside expected voltage range.".format(
        mapping_warnings))
    print("Check jumper positions and cable routing before trusting data.")
    print("REMINDER: Do NOT connect PCIe cables to EPS ports!")
    print("!" * COL_WIDTH)

print()
print("DATA_START")
# sys.stdout.flush() is not implemented on all MicroPython builds;
# use the underlying binary buffer directly for all subsequent output
_stdout_write = sys.stdout.buffer.write

# Per-sensor consecutive failure counter for recovery triggering
fail_counts = {sid: 0 for sid, _, _, _, _ in active_channels}
n_active = len(active_channels)

while True:
    ts_ms = get_timestamp_ms()
    frame = bytearray(2 + FRAME_HDR_SZ + n_active * SENSOR_SZ + 1)
    frame[0] = FRAME_MAGIC[0]
    frame[1] = FRAME_MAGIC[1]
    struct.pack_into(FRAME_HDR_FMT, frame, 2, ts_ms, n_active)
    off = 2 + FRAME_HDR_SZ

    for sid, sensor, inv_shunt, cal_m, cal_c in active_channels:
        status = STATUS_OK
        voltage_V = 0.0
        current_mA = 0.0

        try:
            bus_raw = sensor._read_reg_u(sensor.REG_BUSVOLT)
            if bus_raw is None:
                raise OSError("bus voltage read returned None")

            voltage_V = ((bus_raw & 0xFFF8) >> 3) * 0.004

            if bus_raw & 0x0001:
                # OVF flag set — current register invalid. Report overflow
                # AND clip-high to sensor's configured max_amps so downstream
                # mean power computations are biased slightly LOW (by at most
                # the gap between true current and max_amps) instead of
                # crashed to zero. The status flag still lets consumers
                # filter these samples explicitly if they need to.
                status = STATUS_OVERFLOW
                
                current_mA = sensor.max_amps * 1000.0
                fail_counts[sid] += 1
            elif voltage_V < 0.5:
                # Rail unpowered: force 0mA (avoid positive offset from calibration c)
                fail_counts[sid] = 0
            else:
                shnt_raw = sensor._read_reg_s(sensor.REG_SHUNTVOLT)
                if shnt_raw is None:
                    raise OSError("shunt voltage read returned None")

                # shunt_mv LSB = 0.01mV; calc_mA = shunt_mV / shunt_ohms
                shunt_mv = shnt_raw * 0.01
                calc_ma = shunt_mv * inv_shunt
                current_mA = cal_m * calc_ma + cal_c
                fail_counts[sid] = 0

        except OSError:
            status = STATUS_READFAIL
            voltage_V = 0.0
            current_mA = 0.0
            fail_counts[sid] += 1

            # Attempt reconfiguration after MAX_CONSECUTIVE_FAILS failures
            # Recovers sensors that reset due to rail brownout
            if fail_counts[sid] % MAX_CONSECUTIVE_FAILS == 0:
                sensor._ok = sensor._calibrate()

        id_status = (sid << 5) | (status << 3)
        v = max(0, min(65535, int(round(voltage_V * SCALE_V))))
        i = max(0, min(65535, int(round(current_mA * SCALE_I))))
        struct.pack_into(SENSOR_FMT, frame, off, id_status, v, i)
        off += SENSOR_SZ

    payload = memoryview(frame)[2:off]
    frame[off] = crc8(payload)
    _stdout_write(memoryview(frame)[:off + 1])
