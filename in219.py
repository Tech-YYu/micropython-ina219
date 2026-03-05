from machine import I2C, Pin
import time

class INA219:
    REG_CONFIG      = 0x00
    REG_SHUNTVOLT   = 0x01
    REG_BUSVOLT     = 0x02
    REG_POWER       = 0x03
    REG_CURRENT     = 0x04
    REG_CALIBRATION = 0x05

    def __init__(self, i2c, addr=0x40, shunt_ohms=0.1, max_amps=3.2):
        self.i2c = i2c
        self.addr = addr
        self.shunt_ohms = shunt_ohms
        self.max_amps = max_amps
        self._calibrate()

    # --- Low-level I2C ---
    def _write_register(self, reg, value):
        buf = bytearray(3)
        buf[0] = reg
        buf[1] = (value >> 8) & 0xFF
        buf[2] = value & 0xFF
        self.i2c.writeto(self.addr, buf)

    def _read_register(self, reg):
        self.i2c.writeto(self.addr, bytearray([reg]), False)
        res = self.i2c.readfrom(self.addr, 2)
        raw = (res[0] << 8) | res[1]
        return raw

    def _read_signed(self, reg):
        val = self._read_register(reg)
        if val > 32767:
            val -= 65536
        return val

    # --- Calibration (dynamic, works for any shunt) ---
    def _calibrate(self):
        # Equation 2: smallest useful Current_LSB
        self.current_lsb = self.max_amps / 32768   # A per bit
        self.power_lsb   = self.current_lsb * 20   # W per bit (Equation 3)

        # Equation 1
        cal = int(0.04096 / (self.current_lsb * self.shunt_ohms))
        
        # Config: 32V bus range, PGA /8 (320mV), 12-bit ADC, continuous
        self._write_register(self.REG_CONFIG, 0x399F)
        self._write_register(self.REG_CALIBRATION, cal)

    # --- Readings ---
    def bus_voltage_V(self):
        raw = self._read_register(self.REG_BUSVOLT)
        return (raw >> 3) * 0.004          # 4 mV LSB, shift 3 per datasheet

    def shunt_voltage_mV(self):
        raw = self._read_signed(self.REG_SHUNTVOLT)
        return raw * 0.01                  # 10 µV LSB → mV

    def current_mA(self):
        raw = self._read_signed(self.REG_CURRENT)
        return raw * self.current_lsb * 1000

    def power_mW(self):
        raw = self._read_register(self.REG_POWER)
        return raw * self.power_lsb * 1000

    def is_overflow(self):
        return bool(self._read_register(self.REG_BUSVOLT) & 0x0001)

    def conversion_ready(self):
        return bool(self._read_register(self.REG_BUSVOLT) & 0x0002)


# --- Usage ---
i2c = I2C(0, scl=Pin(22), sda=Pin(21), freq=400_000)
sensor = INA219(i2c, addr=0x40, shunt_ohms=0.1, max_amps=3.2)

while True:
    if sensor.is_overflow():
        print("OVERFLOW - check wiring or increase max_amps")
    else:
        print(f"Bus:     {sensor.bus_voltage_V():.3f} V")
        print(f"Current: {sensor.current_mA():.1f} mA")
        print(f"Power:   {sensor.power_mW():.1f} mW")
        print("---")
    time.sleep(1)
