"""
Usage:
  python logger.py [PORT]
  python logger.py /dev/tty.usbmodem11201
  python logger.py COM3

Output:
  ina219_hires.parquet   -- flushed every FLUSH_EVERY frames
  ina219_hires.csv       -- written on clean exit only

Requirements:
  pip install pyserial pandas pyarrow
"""

import serial
import serial.tools.list_ports
import struct
import signal
import sys
import time
import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------
# Protocol constants -- must match main.py exactly
# ---------------------------------------------------------------
FRAME_MAGIC   = b'\xAA\x55'
FRAME_HDR_FMT = '!IB'         # timestamp_ms(4B) + sensor_count(1B)
SENSOR_FMT    = '!BHH'        # id_status(1B) + voltage(2B) + current(2B)
FRAME_HDR_SZ  = struct.calcsize(FRAME_HDR_FMT)
SENSOR_SZ     = struct.calcsize(SENSOR_FMT)

SCALE_V    = 100
SCALE_I    = 1

STATUS_LABELS = {0: 'ok', 1: 'overflow', 2: 'error', 3: 'read_fail'}

# ---------------------------------------------------------------
# Sensor metadata -- must match SENSORS table in main.py
# ---------------------------------------------------------------
SENSOR_META = [
    {"chip": "U3", "name": "ATX12",  "location": "main"},
    {"chip": "U4", "name": "ATX5",   "location": "main"},
    {"chip": "U5", "name": "BO12",   "location": "main"},
    {"chip": "U6", "name": "BO3.3",  "location": "main"},
    {"chip": "U1", "name": "PCIe_1", "location": "subboard"},
    {"chip": "U2", "name": "PCIe_2", "location": "subboard"},
    {"chip": "U3", "name": "EPS_1",  "location": "subboard"},
    {"chip": "U4", "name": "EPS_2",  "location": "subboard"},
]

# ---------------------------------------------------------------
# Output files and buffering
# ---------------------------------------------------------------
PARQUET_FILE  = "ina219_hires.parquet"
CSV_FILE      = "ina219_hires.csv"
FLUSH_EVERY   = 500       # write Parquet every N frames (~1s at 500Hz)
BAUD_RATE     = 921600

# Parquet schema -- all columns typed explicitly for efficiency
SCHEMA = pa.schema([
    ('pico_ms',       pa.uint32()),   # ms since midnight on Pico -- use for relative/interval timing only
    ('timestamp_iso', pa.string()),   # PC wall clock -- monotonic, use for absolute time
    ('sensor_id',     pa.uint8()),
    ('chip',          pa.string()),
    ('name',          pa.string()),
    ('location',      pa.string()),
    ('voltage_V',     pa.float32()),
    ('current_mA',    pa.float32()),
    ('power_mW',      pa.float32()),
    ('status',        pa.string()),
])


def find_port():
    ports = serial.tools.list_ports.comports()
    for p in ports:
        if any(k in p.description for k in
               ['USB Serial', 'UART', 'CH340', 'CP210', 'Pico', 'MicroPython']):
            return p.device
    return None


def crc8(data):
    crc = 0
    for b in data:
        crc ^= b
    return crc


# ---------------------------------------------------------------
# ASCII startup handshake (sync + latency)
# Identical protocol to original logger.py.
# Returns when DATA_START line is received.
# ---------------------------------------------------------------
def handle_startup(ser, timeout_s=60):
    latency_ms = 0.0
    print("Waiting for Pico startup...")
    t_start = time.monotonic()

    while True:
        if time.monotonic() - t_start > timeout_s:
            raise RuntimeError(
                f"Startup timeout: DATA_START not received within {timeout_s}s. "
                "Is the Pico running main.py?"
            )
        raw = ser.readline().decode('utf-8', errors='ignore').strip()
        if not raw:
            continue

        print(f"[MCU] {raw}")

        if raw == "SYNC_REQUEST":
            now = datetime.now()
            ts  = now.strftime('%Y-%m-%d %H:%M:%S.') + f"{now.microsecond // 1000:03d}"
            ser.write(f"SYNC,{ts}\n".encode())
            ser.flush()
            print(f"[PC]  Sent SYNC: {ts}")

        elif raw.startswith("PING,"):
            idx = raw.split(",")[1]
            ser.write(f"PONG,{idx}\n".encode())
            ser.flush()

        elif raw.startswith("LATENCY,"):
            parts      = raw.split(",")
            latency_ms = float(parts[1].replace("ms", ""))
            print(f"[PC]  One-way latency: {latency_ms:.2f} ms")

        elif raw == "DATA_START":
            print("[PC]  Startup complete. Switching to binary frame parser.")
            print()
            break

    return latency_ms


# ---------------------------------------------------------------
# Binary frame parser
# ---------------------------------------------------------------
def parse_frame(data):
    """
    Parse one complete binary frame (payload + CRC, magic already stripped).
    Returns list of row dicts, or None on CRC error.
    """
    payload  = data[:-1]
    crc_rx   = data[-1]
    if crc8(payload) != crc_rx:
        return None

    ts_ms, n_sensors = struct.unpack_from(FRAME_HDR_FMT, payload, 0)
    offset = FRAME_HDR_SZ

    # Use PC wall clock entirely for ts_iso 
    now    = datetime.now()
    ts_iso = now.strftime('%Y-%m-%d %H:%M:%S.') + f"{now.microsecond // 1000:03d}"

    readings = []
    for _ in range(n_sensors):
        id_status, v_raw, i_raw = struct.unpack_from(SENSOR_FMT, payload, offset)
        offset += SENSOR_SZ

        sensor_id  = (id_status >> 5) & 0x07
        status_raw = (id_status >> 3) & 0x03

        if sensor_id >= len(SENSOR_META):
            continue

        meta       = SENSOR_META[sensor_id]
        voltage_V  = v_raw / SCALE_V
        current_mA = i_raw / SCALE_I
        power_mW   = voltage_V * current_mA   # derived: P = V * I

        readings.append({
            'pico_ms':       ts_ms,
            'timestamp_iso': ts_iso,
            'sensor_id':     sensor_id,
            'chip':          meta['chip'],
            'name':          meta['name'],
            'location':      meta['location'],
            'voltage_V':     voltage_V,
            'current_mA':    current_mA,
            'power_mW':      power_mW,
            'status':        STATUS_LABELS.get(status_raw, 'unknown'),
        })

    return readings


# ---------------------------------------------------------------
# Parquet writer (incremental, in-memory buffer)
# ---------------------------------------------------------------
class ParquetWriter:
    def __init__(self, path, schema, flush_every):
        self.path        = path
        self.schema      = schema
        self.flush_every = flush_every
        self.writer      = None
        self.buffer      = []
        self.total_rows  = 0
        self.frame_count = 0

    def append(self, rows):
        self.buffer.extend(rows)
        self.frame_count += 1
        if self.frame_count % self.flush_every == 0:
            self._flush()

    def _flush(self):
        if not self.buffer:
            return
        table = pa.Table.from_pylist(self.buffer, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.path, self.schema,
                                           compression='snappy')
        self.writer.write_table(table)
        self.total_rows += len(self.buffer)
        size_kb = os.path.getsize(self.path) / 1024
        print(f"  -> Parquet flushed: {self.total_rows} rows  {size_kb:.1f} KB")
        self.buffer = []

    def flush_if_pending(self):
        """Flush only if there is unsaved data -- safe to call at any time."""
        if self.buffer:
            self._flush()

    def close(self):
        self._flush()
        if self.writer:
            self.writer.close()
            self.writer = None


# ---------------------------------------------------------------
# Connection helper -- separated so it can be retried on reconnect
# ---------------------------------------------------------------
RECONNECT_DELAY_S = 5      # seconds between reconnect attempts
MAX_RECONNECTS    = 12     # give up after this many consecutive failures (~1 min)


def connect(port):
    """
    Open serial port and run ASCII startup handshake.
    Returns (ser, latency_ms) on success.
    Raises serial.SerialException or RuntimeError on failure.
    """
    ser = serial.Serial(port, BAUD_RATE, timeout=2, write_timeout=2)
    time.sleep(0.5)
    latency_ms = handle_startup(ser)
    return ser, latency_ms


def resolve_port():
    """Return port from auto-detect or CLI arg, or exit."""
    port = find_port()
    if not port:
        if len(sys.argv) > 1:
            port = sys.argv[1]
        else:
            print("No port found. Usage: python logger.py COM3")
            sys.exit(1)
    return port


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------
def main():
    port = resolve_port()

    # Stats -- shared across reconnects so totals are cumulative
    frame_count    = 0
    crc_errors     = 0
    reconnect_count = 0
    t_start        = time.monotonic()
    t_last_report  = t_start

    # Parquet writer -- one instance for the entire run, survives reconnects
    writer = ParquetWriter(PARQUET_FILE, SCHEMA, FLUSH_EVERY)

    # ---------------------------------------------------------------
    # Cleanup -- called on KeyboardInterrupt, SIGTERM, or final exit
    # ---------------------------------------------------------------
    def cleanup(reason="exit"):
        elapsed = time.monotonic() - t_start
        rate    = frame_count / elapsed if elapsed > 0 else 0
        print(f"\n[{reason}]")
        print(f"  Frames received  : {frame_count}")
        print(f"  CRC errors       : {crc_errors}")
        print(f"  Reconnects       : {reconnect_count}")
        print(f"  Average rate     : {rate:.1f} Hz")
        print(f"  Duration         : {elapsed:.1f}s")
        writer.close()
        if frame_count > 0:
            try:
                df = pd.read_parquet(PARQUET_FILE)
                df.to_csv(CSV_FILE, index=False)
                csv_kb = os.path.getsize(CSV_FILE) / 1024
                print(f"  CSV written      : {CSV_FILE}  ({csv_kb:.1f} KB)")
            except Exception as e:
                print(f"  CSV write failed : {e}")
        print("Done.")

    # Wire SIGTERM / SIGHUP to cleanup -- ser may not exist yet so close
    # is deferred; the serial port will be closed by the OS on process exit.
    signal.signal(signal.SIGTERM, lambda s, f: (cleanup("SIGTERM"), sys.exit(0)))
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, lambda s, f: (cleanup("SIGHUP"), sys.exit(0)))

    # Console header (printed once)
    print(f"Logging to : {PARQUET_FILE}")
    print(f"Flushing every {FLUSH_EVERY} frames (~1s at 500 Hz)\n")
    print(f"{'Timestamp':<27} {'Name':<10} {'Voltage':>9} "
          f"{'Current':>10} {'Power':>10} {'Status'}")
    print("-" * 80)

    # ---------------------------------------------------------------
    # Outer reconnect loop
    # ---------------------------------------------------------------
    ser = None
    try:
        while True:
            # --- Connect / reconnect ---
            consecutive_failures = 0
            while True:
                try:
                    print(f"\nConnecting to {port}..." if reconnect_count == 0
                          else f"\nReconnecting to {port} "
                               f"(attempt {consecutive_failures + 1}/{MAX_RECONNECTS})...")
                    ser, latency_ms = connect(port)
                    print(f"Connected. Latency: {latency_ms:.2f} ms\n")
                    consecutive_failures = 0
                    break   # connected -- enter frame loop

                except (serial.SerialException, RuntimeError, OSError) as e:
                    consecutive_failures += 1
                    reconnect_count      += 1
                    writer.flush_if_pending()   # save whatever we have
                    print(f"  Connection failed: {e}")
                    if consecutive_failures >= MAX_RECONNECTS:
                        print(f"  Giving up after {MAX_RECONNECTS} attempts.")
                        cleanup("MaxReconnects")
                        return
                    print(f"  Retrying in {RECONNECT_DELAY_S}s...")
                    time.sleep(RECONNECT_DELAY_S)

            # --- Binary frame receive loop ---
            buf        = bytearray()
            buf_offset = 0

            try:
                while True:
                    chunk = ser.read(ser.in_waiting or 1)
                    if not chunk:
                        continue
                    buf.extend(chunk)

                    # Scan for complete frames
                    while True:
                        magic_pos = buf.find(FRAME_MAGIC, buf_offset)
                        if magic_pos == -1:
                            buf        = bytearray()
                            buf_offset = 0
                            break
                        if magic_pos > buf_offset:
                            buf_offset = magic_pos

                        min_hdr = buf_offset + len(FRAME_MAGIC) + FRAME_HDR_SZ
                        if len(buf) < min_hdr:
                            break

                        hdr_start    = buf_offset + len(FRAME_MAGIC)
                        _, n_sensors = struct.unpack_from(FRAME_HDR_FMT, buf, hdr_start)
                        frame_size   = len(FRAME_MAGIC) + FRAME_HDR_SZ + n_sensors * SENSOR_SZ + 1

                        if len(buf) - buf_offset < frame_size:
                            break

                        payload_start = buf_offset + len(FRAME_MAGIC)
                        frame_data    = bytes(buf[payload_start : buf_offset + frame_size])
                        buf_offset   += frame_size

                        if buf_offset > 4096:
                            buf        = buf[buf_offset:]
                            buf_offset = 0

                        rows = parse_frame(frame_data)
                        if rows is None:
                            crc_errors += 1
                            continue

                        frame_count += 1
                        writer.append(rows)

                        if frame_count % 50 == 0:
                            for row in rows[:1]:
                                status_str = row['status'].upper() if row['status'] != 'ok' else 'ok'
                                print(
                                    f"{row['timestamp_iso']:<27} "
                                    f"{row['name']:<10} "
                                    f"{row['voltage_V']:>8.3f}V "
                                    f"{row['current_mA']:>8.1f}mA "
                                    f"{row['power_mW']:>8.1f}mW "
                                    f"{status_str}"
                                )

                        now = time.monotonic()
                        if now - t_last_report >= 10.0:
                            elapsed  = now - t_start
                            rate     = frame_count / elapsed
                            print(f"  -- {frame_count} frames  {rate:.1f} Hz  "
                                  f"{crc_errors} CRC errors  "
                                  f"{reconnect_count} reconnects --")
                            t_last_report = now

            except serial.SerialException as e:
                print(f"\n[SerialException] {e}")
                print(f"  Frames so far: {frame_count}  CRC errors: {crc_errors}")
                try:
                    ser.close()
                except Exception:
                    pass
                ser = None
                # Fall through to outer reconnect loop

    except KeyboardInterrupt:
        cleanup("KeyboardInterrupt")
    finally:
        if ser and ser.is_open:
            ser.close()


if __name__ == "__main__":
    main()