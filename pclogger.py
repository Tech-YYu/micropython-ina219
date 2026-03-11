"""
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
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------
# Protocol constants -- must match main.py exactly
# ---------------------------------------------------------------
FRAME_MAGIC   = b'\xAA\x55'
FRAME_HDR_FMT = '!IB'          # timestamp_ms(4B) + sensor_count(1B)
SENSOR_FMT    = '!BHHIhHB'    # id + v + i + p + sh + sv + status
FRAME_HDR_SZ  = struct.calcsize(FRAME_HDR_FMT)
SENSOR_SZ     = struct.calcsize(SENSOR_FMT)

SCALE_V    = 100
SCALE_I    = 1
SCALE_P    = 1
SCALE_SH   = 10
SCALE_SV   = 100

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
FLUSH_EVERY   = 500       # write Parquet every N frames (~5s at 100Hz)
BAUD_RATE     = 921600

# Parquet schema -- all columns typed explicitly for efficiency
SCHEMA = pa.schema([
    ('timestamp_ms',  pa.uint32()),
    ('timestamp_iso', pa.string()),
    ('sensor_id',     pa.uint8()),
    ('chip',          pa.string()),
    ('name',          pa.string()),
    ('location',      pa.string()),
    ('voltage_V',     pa.float32()),
    ('current_mA',    pa.float32()),
    ('power_mW',      pa.float32()),
    ('shunt_mV',      pa.float32()),
    ('supply_V',      pa.float32()),
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
    Parse one complete binary frame.

    data: bytes starting after magic (i.e. the payload + CRC).
    Returns list of dicts, one per sensor, or None on CRC error.
    """
    # CRC covers everything between magic and CRC byte
    payload    = data[:-1]
    crc_rx     = data[-1]
    crc_calc   = crc8(payload)

    if crc_rx != crc_calc:
        return None   # CRC mismatch -- discard frame

    offset = 0
    ts_ms, n_sensors = struct.unpack_from(FRAME_HDR_FMT, payload, offset)
    offset += FRAME_HDR_SZ

    # Reconstruct ISO timestamp from ms field
    # ms field is (sec % 60)*1000 + ms_within_sec, not absolute epoch
    # We reconstruct wall-clock time from PC clock at parse time,
    # then use ts_ms for sub-second precision alignment.
    now      = datetime.now()
    sec_part = ts_ms // 1000
    ms_part  = ts_ms % 1000
    ts_iso   = now.strftime('%Y-%m-%d %H:%M:%S.') + f"{ms_part:03d}"

    readings = []
    for _ in range(n_sensors):
        sensor_id, v_raw, i_raw, p_raw, sh_raw, sv_raw, status_raw = \
            struct.unpack_from(SENSOR_FMT, payload, offset)
        offset += SENSOR_SZ

        if sensor_id >= len(SENSOR_META):
            continue   # unknown sensor index -- skip

        meta = SENSOR_META[sensor_id]

        readings.append({
            'timestamp_ms':  ts_ms,
            'timestamp_iso': ts_iso,
            'sensor_id':     sensor_id,
            'chip':          meta['chip'],
            'name':          meta['name'],
            'location':      meta['location'],
            'voltage_V':     v_raw  / SCALE_V,
            'current_mA':    i_raw  / SCALE_I,
            'power_mW':      p_raw  / SCALE_P,
            'shunt_mV':      sh_raw / SCALE_SH,
            'supply_V':      sv_raw / SCALE_SV,
            'status':        STATUS_LABELS.get(status_raw, 'unknown'),
        })

    return readings


# ---------------------------------------------------------------
# Parquet writer 
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

    def close(self):
        self._flush()
        if self.writer:
            self.writer.close()
            self.writer = None


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------
def main():
    # Port selection
    port = find_port()
    if not port:
        if len(sys.argv) > 1:
            port = sys.argv[1]
        else:
            print("No port found. Usage: python logger.py COM3")
            sys.exit(1)

    print(f"Connecting to {port} at {BAUD_RATE} baud...")
    try:
        ser = serial.Serial(port, BAUD_RATE, timeout=2, write_timeout=2)
    except serial.SerialException as e:
        print(f"Failed to open port: {e}")
        sys.exit(1)

    time.sleep(0.5)
    print("Connected.\n")

    # ASCII startup handshake
    latency_ms = handle_startup(ser)
    print(f"Latency compensation: {latency_ms:.2f} ms")
    print(f"Logging to: {PARQUET_FILE}")
    print(f"Flushing every {FLUSH_EVERY} frames\n")

    # Stats
    frame_count   = 0
    crc_errors    = 0
    t_start       = time.monotonic()
    t_last_report = t_start

    # Parquet writer
    writer = ParquetWriter(PARQUET_FILE, SCHEMA, FLUSH_EVERY)

    # Console header
    print(f"{'Timestamp':<27} {'Name':<10} {'Voltage':>9} "
          f"{'Current':>10} {'Power':>10} {'Status'}")
    print("-" * 80)

    # Cleanup on exit
    def cleanup(reason="exit"):
        nonlocal frame_count, crc_errors
        elapsed = time.monotonic() - t_start
        rate    = frame_count / elapsed if elapsed > 0 else 0
        print(f"\n{reason}")
        print(f"  Frames received : {frame_count}")
        print(f"  CRC errors      : {crc_errors}")
        print(f"  Average rate    : {rate:.1f} Hz")
        print(f"  Duration        : {elapsed:.1f}s")
        writer.close()
        # Write CSV on exit for human inspection
        if frame_count > 0:
            try:
                df = pd.read_parquet(PARQUET_FILE)
                df.to_csv(CSV_FILE, index=False)
                csv_kb = os.path.getsize(CSV_FILE) / 1024
                print(f"  CSV written     : {CSV_FILE}  ({csv_kb:.1f} KB)")
            except Exception as e:
                print(f"  CSV write failed: {e}")
        print("Done.")
        ser.close()

    signal.signal(signal.SIGTERM, lambda s, f: (cleanup("SIGTERM"), sys.exit(0)))
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, lambda s, f: (cleanup("SIGHUP"), sys.exit(0)))

    # ---------------------------------------------------------------
    # Binary frame receive loop
    # ---------------------------------------------------------------
    buf    = bytearray()
    buf_offset = 0   # index into buf -- avoids repeated bytearray copies

    try:
        while True:
            chunk = ser.read(ser.in_waiting or 1)
            if not chunk:
                continue
            buf.extend(chunk)

            # Scan for complete frames
            while True:
                # Find magic bytes starting from current offset
                magic_pos = buf.find(FRAME_MAGIC, buf_offset)
                if magic_pos == -1:
                    buf        = bytearray()   # no magic anywhere -- discard all
                    buf_offset = 0
                    break
                if magic_pos > buf_offset:
                    buf_offset = magic_pos     # skip garbage before magic

                # Need at least magic + header to read sensor count
                min_hdr = buf_offset + len(FRAME_MAGIC) + FRAME_HDR_SZ
                if len(buf) < min_hdr:
                    break   # wait for more data

                # Peek at sensor count
                hdr_start  = buf_offset + len(FRAME_MAGIC)
                _, n_sensors = struct.unpack_from(FRAME_HDR_FMT, buf, hdr_start)
                frame_size   = len(FRAME_MAGIC) + FRAME_HDR_SZ + n_sensors * SENSOR_SZ + 1

                if len(buf) - buf_offset < frame_size:
                    break   # frame not yet complete

                # Extract frame payload (everything after magic, including CRC)
                payload_start = buf_offset + len(FRAME_MAGIC)
                frame_data    = bytes(buf[payload_start : buf_offset + frame_size])
                buf_offset   += frame_size

                # Reclaim memory once we've consumed a large chunk
                if buf_offset > 4096:
                    buf        = buf[buf_offset:]
                    buf_offset = 0

                # Parse
                rows = parse_frame(frame_data)
                if rows is None:
                    crc_errors += 1
                    continue

                frame_count += 1
                writer.append(rows)

                # Console output throttled to ~10 Hz (every 10 frames)
                # Printing at 99 Hz floods the terminal and adds latency
                if frame_count % 10 == 0:
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

                # Periodic rate report
                now = time.monotonic()
                if now - t_last_report >= 10.0:
                    elapsed  = now - t_start
                    rate     = frame_count / elapsed
                    print(f"  -- {frame_count} frames  {rate:.1f} Hz  "
                          f"{crc_errors} CRC errors --")
                    t_last_report = now

    except KeyboardInterrupt:
        cleanup("KeyboardInterrupt")
    except serial.SerialException as e:
        print(f"\nSerial error: {e}")
        cleanup("SerialException")


if __name__ == "__main__":
    main()