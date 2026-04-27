"""
Usage:
  python3 -u pclogger.py [PORT]
  python3 -u pclogger.py /dev/ttyACM0
  python3 -u pclogger.py /dev/tty.usbmodem11201
  python3 -u pclogger.py COM3

Output:
  ina219_hires.parquet   -- flushed every FLUSH_EVERY frames
  ina219_hires.csv       -- written continuously, crash-safe (primary fallback)

Bench sampling rate (PC receive rate + Pico inter-frame timing from pico_ms_ext):
  python3 -u pclogger.py --bench 15
  python3 -u pclogger.py /dev/ttyACM0 --bench 10 --bench-no-disk

Requirements:
  pip install pyserial pandas pyarrow
"""

import argparse
import csv
import queue
import serial
import serial.tools.list_ports
import struct
import signal
import sys
import threading
import time
import os
from datetime import datetime

import pandas as pd
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PYARROW_AVAILABLE = False

# ---------------------------------------------------------------
# Protocol constants -- must match main.py exactly
# ---------------------------------------------------------------
FRAME_MAGIC   = b'\xAA\x55'
FRAME_HDR_FMT = '!IB'         # timestamp_ms(4B) + sensor_count(1B)
SENSOR_FMT    = '!BHH'        # id_status(1B) + voltage(2B) + current(2B)
FRAME_HDR_SZ  = struct.calcsize(FRAME_HDR_FMT)
SENSOR_SZ     = struct.calcsize(SENSOR_FMT)

SCALE_V    = 100
# SCALE_I=1 is intentional: firmware packs calibrated current directly in mA units
# into SENSOR_FMT field i_raw, so logger conversion is 1:1 (i_raw -> mA).
SCALE_I    = 1

STATUS_LABELS = {0: 'ok', 1: 'overflow', 2: 'error', 3: 'read_fail'}

# ---------------------------------------------------------------
# Sensor metadata -- must match SENSORS table in in219.py (indexed by stable_id)
# ---------------------------------------------------------------
SENSOR_META = [
    {"chip": "U1", "name": "PCIe",  "location": "pcie_eps"},   # id=0, 0x44
    {"chip": "U2", "name": "EPS",   "location": "pcie_eps"},   # id=1, 0x45
    {"chip": "U3", "name": "ATX24", "location": "atx24"},      # id=2, 0x40
    {"chip": "U4", "name": "ATX24", "location": "atx24"},      # id=3, 0x41
    {"chip": "U5", "name": "BO12",  "location": "atx24"},      # id=4, 0x42
    {"chip": "U6", "name": "BO3.3", "location": "atx24"},      # id=5, 0x43
    # {"chip": "U7", "name": "EPS-D", "location": "db1_or_db2"},   # id=6, 0x4A — db1 / db2 (3.333mΩ)
    {"chip": "U8", "name": "EPS-D", "location": "db3"},            # id=6, 0x4A — db3     (1.667mΩ)  ← active
]

# ---------------------------------------------------------------
# Output files and buffering
# ---------------------------------------------------------------
PARQUET_FILE  = "ina219_hires.parquet"
CSV_FILE      = "ina219_hires.csv"
FLUSH_EVERY   = 1000      # write Parquet every N frames (reduced from 10000 for safety)
BAUD_RATE     = 921600
MARKERS_FILE  = "phase_markers.csv"

# Throughput tuning
SERIAL_TIMEOUT_S     = 0.02   # short timeout prevents long blocking reads
MAX_READ_CHUNK       = 16384  # larger reads reduce Python call overhead
REPORT_INTERVAL_S    = 60.0   # periodic stats report cadence
BENCH_REPORT_INTERVAL_S = 1.0  # during --bench, progress print cadence
SAMPLE_PRINT_EVERY   = 0      # 0 disables per-sample console printing

# Robustness guards
BUF_MAX_BYTES              = 1_000_000   # safety cap for malformed/corrupt streams
PICO_ROLLOVER_THRESHOLD_MS = 1_000_000   # detect large backward jump in pico_ms
# MicroPython ticks_ms() rolls over at 2^30 = 1,073,741,824 ms (~12.4 days).
# This is NOT 24 hours — it is the RP2040 port's native ticks_ms() modulus.
# Using 86_400_000 (1 day) here would corrupt pico_ms_ext on the first real rollover.
PICO_TICKS_MODULUS_MS      = 1_073_741_824  # 2^30
SUSPICIOUS_WARN_INTERVAL_S = 5.0         # throttle warning prints

# Canonical column order (used by both Parquet SCHEMA and CSV header)
CSV_FIELDS = [
    'pico_ms',       # ticks_ms() on Pico — monotonic, use for relative/interval timing only
    'pico_epoch',    # rollover epoch counter (increments every ~12.4 days)
    'pico_ms_ext',   # extended monotonic ms = pico_epoch * 2^30 + pico_ms
    'timestamp_iso', # PC wall clock (datetime); can step if NTP adjusts mid-run
    'pc_mono_ns',    # time.perf_counter_ns() paired with timestamp_iso
    'sensor_id',
    'chip',
    'name',
    'location',
    'voltage_V',
    'current_mA',
    'power_mW',
    'status',
]

# Parquet schema — only built when pyarrow is available; CSV path works regardless
if PYARROW_AVAILABLE:
    SCHEMA = pa.schema([
        ('pico_ms',       pa.uint32()),
        ('pico_epoch',    pa.uint32()),
        ('pico_ms_ext',   pa.uint64()),
        ('timestamp_iso', pa.string()),
        ('pc_mono_ns',    pa.int64()),
        ('sensor_id',     pa.uint8()),
        ('chip',          pa.string()),
        ('name',          pa.string()),
        ('location',      pa.string()),
        ('voltage_V',     pa.float32()),
        ('current_mA',    pa.float32()),
        ('power_mW',      pa.float32()),
        ('status',        pa.string()),
    ])
    assert [f.name for f in SCHEMA] == CSV_FIELDS, \
        "SCHEMA / CSV_FIELDS drifted; keep column order in sync."
else:
    SCHEMA = None

# Raspberry Pi USB vendor — Pico CDC ACM (MicroPython, etc.) reports this when enumerated.
RPI_USB_VID = 0x2E8A


def _port_identity(p):
    """Single string for keyword matching (Linux/macOS/Windows)."""
    parts = [p.device, p.description or "", p.manufacturer or "", p.product or "", p.hwid or ""]
    return " ".join(parts)


def find_port():
    """
    Prefer Raspberry Pi USB VID, then Pico/MicroPython-like names, then legacy heuristics.
    Avoids picking a random USB–UART adapter when a Pico is also connected.
    """
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        return None

    for p in ports:
        if p.vid == RPI_USB_VID:
            return p.device

    keywords = (
        "pico", "micropython", "circuitpython", "rp2040", "raspberry pi",
        "usb serial device",  # Linux generic CDC name for Pico
    )
    lower = _port_identity
    for p in ports:
        ident = lower(p).lower()
        if any(k in ident for k in keywords):
            return p.device

    for p in ports:
        desc = p.description or ""
        if any(k in desc for k in ("USB Serial", "UART", "CH340", "CP210", "Pico", "MicroPython")):
            return p.device
    return None


def print_serial_ports():
    """Print all serial devices (for choosing PORT or debugging permissions)."""
    ports = list(serial.tools.list_ports.comports())
    if not ports:
        print("No serial ports found.")
        return
    for p in ports:
        vid = f"0x{p.vid:04X}" if p.vid is not None else "----"
        pid = f"0x{p.pid:04X}" if p.pid is not None else "----"
        m = p.manufacturer or "?"
        prod = p.product or "?"
        print(f"{p.device}")
        print(f"  vid:pid {vid}:{pid}  {m} — {prod}")
        print(f"  {p.description or ''}")


def now_iso_and_mono():
    """
    Wall-clock string and monotonic nanoseconds from the same instant.
    perf_counter_ns does not jump when NTP steps datetime; use pc_mono_ns to align with sync files
    that record perf_counter_ns on the same machine.
    """
    mono = time.perf_counter_ns()
    now = datetime.now()
    iso = now.strftime('%Y-%m-%d %H:%M:%S.') + f"{now.microsecond // 1000:03d}"
    return iso, mono


def now_iso_ms():
    """Wall-clock timestamp only (e.g. markers). For frames use now_iso_and_mono()."""
    return now_iso_and_mono()[0]


def parse_args():
    parser = argparse.ArgumentParser(
        description="High-throughput Pico power logger with optional phase markers."
    )
    parser.add_argument(
        "port",
        nargs="?",
        default=None,
        help="Serial port (e.g. Linux /dev/ttyACM0, macOS /dev/cu.usbmodem21301). Auto-detect if omitted.",
    )
    parser.add_argument(
        "--markers-file",
        default=MARKERS_FILE,
        help=f"CSV file to store phase markers (default: {MARKERS_FILE})",
    )
    parser.add_argument(
        "--no-markers",
        action="store_true",
        help="Disable interactive marker capture from stdin.",
    )
    parser.add_argument(
        "--list-ports",
        action="store_true",
        help="List serial devices (path, USB vid:pid, description) and exit.",
    )
    parser.add_argument(
        "--bench",
        type=float,
        nargs="?",
        const=10.0,
        default=None,
        metavar="SECONDS",
        help="Benchmark: after DATA_START, run for SECONDS (default 10), print sampling stats, exit.",
    )
    parser.add_argument(
        "--bench-no-disk",
        action="store_true",
        help="With --bench, skip CSV/Parquet (measure USB + parse + Python only).",
    )
    return parser.parse_args()


class MarkerLogger:
    """
    Cross-platform interactive marker capture.

    A background daemon thread reads sys.stdin line-by-line (blocking is fine
    in a dedicated thread). Parsed commands are pushed into a thread-safe Queue.
    The main loop calls poll() each iteration to drain the queue without blocking.

    This approach works on Linux, macOS, and Windows — unlike select.select()
    which only works on sockets on Windows, not on file descriptors like stdin.
    """

    def __init__(self, path, enabled=True):
        self.path    = path
        self.enabled = enabled
        self._queue  = queue.Queue()
        self._thread = None

        if not self.enabled:
            return

        new_file = not os.path.exists(self.path)
        with open(self.path, "a", encoding="utf-8") as f:
            if new_file:
                f.write("timestamp_iso,event,source\n")
        print(f"Markers file: {self.path}")
        print("Marker commands: 'm <label>' (or 'mark <label>'), 'help'")

        # Daemon thread exits automatically when main process exits
        self._thread = threading.Thread(target=self._stdin_reader, daemon=True)
        self._thread.start()

    def _stdin_reader(self):
        """Blocking stdin reader running in background daemon thread."""
        while True:
            try:
                line = sys.stdin.readline()
            except Exception:
                break
            if not line:
                # EOF — stdin closed (e.g. piped input exhausted)
                break
            cmd = line.strip()
            if cmd:
                self._queue.put(cmd)

    def add(self, label, source="manual"):
        if not self.enabled:
            return
        label = (label or "").strip()
        if not label:
            return
        ts   = now_iso_ms()
        safe = '"' + label.replace('"', '""') + '"'
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(f"{ts},{safe},{source}\n")
        print(f"[MARK] {ts}  {label}")

    def poll(self):
        """
        Drain the stdin queue without blocking. Called from the main loop.
        Replaces the old poll_stdin() which used select.select() and failed on Windows.
        """
        if not self.enabled:
            return
        while True:
            try:
                cmd = self._queue.get_nowait()
            except queue.Empty:
                break
            low = cmd.lower()
            if low in ("help", "?"):
                print("Marker commands: m <label> | mark <label>")
            elif low.startswith("m "):
                self.add(cmd[2:], source="manual")
            elif low.startswith("mark "):
                self.add(cmd[5:], source="manual")
            else:
                self.add(cmd, source="manual")


def xor_checksum(data):
    # NOTE: This is an XOR/LRC checksum, NOT a polynomial CRC-8.
    # It cannot detect swapped bytes or inserted zero-bytes.
    # Must match the checksum function in the Pico firmware exactly.
    # To upgrade to a real CRC-8 (e.g. Dallas/Maxim 0x31 polynomial),
    # the firmware must be updated simultaneously.
    crc = 0
    for b in data:
        crc ^= b
    return crc


# ---------------------------------------------------------------
# ASCII startup handshake (sync + latency)
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
            print(
                "[PC]  Time: timestamp_iso + pc_mono_ns = wall + perf_counter_ns; "
                "pico_ms_ext = Pico monotonic — align sync files via pc_mono_ns on this host."
            )
            print()
            break

    return latency_ms


# ---------------------------------------------------------------
# Binary frame parser
# ---------------------------------------------------------------
def parse_frame(data, timestamp_iso, pico_state, pc_mono_ns):
    """
    Parse one complete binary frame (payload + CRC, magic already stripped).
    timestamp_iso and pc_mono_ns are captured at chunk arrival time (before parsing work).
    pico_state tracks rollover epochs for pico_ms.
    Returns list of row dicts, or None on CRC error.
    """
    payload  = data[:-1]
    crc_rx   = data[-1]
    if xor_checksum(payload) != crc_rx:
        return None

    ts_ms, n_sensors = struct.unpack_from(FRAME_HDR_FMT, payload, 0)
    offset = FRAME_HDR_SZ

    prev_ts = pico_state["prev_ts_ms"]
    if prev_ts is not None and ts_ms < (prev_ts - PICO_ROLLOVER_THRESHOLD_MS):
        pico_state["pico_epoch"] += 1
        print(f"[WARN] pico_ms rollover detected -> epoch={pico_state['pico_epoch']}")
    pico_state["prev_ts_ms"] = ts_ms

    pico_epoch  = pico_state["pico_epoch"]
    pico_ms_ext = pico_epoch * PICO_TICKS_MODULUS_MS + ts_ms

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
        power_mW   = voltage_V * current_mA

        readings.append({
            'pico_ms':       ts_ms,
            'pico_epoch':    pico_epoch,
            'pico_ms_ext':   pico_ms_ext,
            'timestamp_iso': timestamp_iso,
            'pc_mono_ns':    pc_mono_ns,
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
# CsvWriter -- crash-safe, append-mode, line-buffered
# Primary fallback if Parquet footer is lost on hard crash.
# ---------------------------------------------------------------
class CsvWriter:
    """
    Writes every frame to CSV immediately with line-buffering + explicit flush.
    Survives SIGKILL and hard crashes: each row is in the OS page cache
    before the next frame is processed, so the file is always readable.
    """

    def __init__(self, path: str):
        self.path = path
        new_file  = not os.path.exists(path)
        # buffering=1 = line-buffered; combined with explicit flush = crash-safe
        self._f      = open(path, "a", newline="", buffering=1, encoding="utf-8")
        self._writer = csv.DictWriter(self._f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if new_file:
            self._writer.writeheader()
            self._f.flush()

    def append(self, rows):
        for row in rows:
            self._writer.writerow(row)
        self._f.flush()   # guarantee OS has the data before we process the next frame

    def close(self):
        try:
            self._f.flush()
            self._f.close()
        except Exception:
            pass


# ---------------------------------------------------------------
# ParquetWriter -- efficient columnar format, flushed periodically
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
            self.writer = pq.ParquetWriter(self.path, self.schema, compression=None)
        self.writer.write_table(table)
        self.total_rows += len(self.buffer)
        print(f"  -> Parquet flushed: {self.total_rows} rows")
        self.buffer = []

    def flush_if_pending(self):
        if self.buffer:
            self._flush()

    def close(self):
        try:
            self._flush()
        except Exception as e:
            print(f"[WARN] ParquetWriter flush on close failed: {e}")
        if self.writer:
            try:
                self.writer.close()
            except Exception as e:
                print(f"[WARN] ParquetWriter.close() failed: {e}")
            self.writer = None


class NullCsvWriter:
    """No-op sink for --bench --bench-no-disk."""

    def append(self, rows):
        pass

    def close(self):
        pass


class NullParquetWriter:
    """No-op sink for --bench --bench-no-disk."""

    def __init__(self):
        self.frame_count = 0

    def append(self, rows):
        self.frame_count += 1

    def flush_if_pending(self):
        pass

    def close(self):
        pass


def bench_stats_init():
    return {"n": 0, "sum_dt": 0.0, "sum_sq": 0.0, "min_dt": None, "max_dt": None}


def bench_stats_add(st, dt_ms):
    if dt_ms is None or dt_ms <= 0:
        return
    st["n"] += 1
    st["sum_dt"] += dt_ms
    st["sum_sq"] += dt_ms * dt_ms
    if st["min_dt"] is None or dt_ms < st["min_dt"]:
        st["min_dt"] = dt_ms
    if st["max_dt"] is None or dt_ms > st["max_dt"]:
        st["max_dt"] = dt_ms


def format_bench_summary(st, frame_count, wall_s, crc_errors, reconnect_count):
    """Human-readable bench report (wall_s = monotonic span of the measurement loop)."""
    lines = []
    lines.append("")
    lines.append("=== Bench summary ===")
    if wall_s > 0:
        pc_hz = frame_count / wall_s
        lines.append(f"  PC receive rate     : {pc_hz:.1f} Hz  ({frame_count} frames / {wall_s:.3f} s wall)")
    else:
        lines.append(f"  PC receive rate     : (no time elapsed)  frames={frame_count}")
    lines.append(f"  CRC errors          : {crc_errors}")
    lines.append(f"  Reconnects          : {reconnect_count}")
    n = st["n"]
    if n == 0:
        lines.append("  Pico inter-frame dt : (no deltas — need >= 2 frames)")
        lines.append("=== End bench ===")
        return "\n".join(lines)
    mean_dt = st["sum_dt"] / n
    var = max(0.0, st["sum_sq"] / n - mean_dt * mean_dt)
    std_dt = var ** 0.5
    mean_hz = 1000.0 / mean_dt if mean_dt > 0 else 0.0
    # Shortest gap -> highest instantaneous Hz; longest gap -> lowest instantaneous Hz
    hi_hz = 1000.0 / st["min_dt"] if st["min_dt"] and st["min_dt"] > 0 else 0.0
    lo_hz = 1000.0 / st["max_dt"] if st["max_dt"] and st["max_dt"] > 0 else 0.0
    lines.append(
        f"  Pico inter-frame dt : mean {mean_dt:.3f} ms  std {std_dt:.3f} ms  "
        f"min {st['min_dt']:.3f} ms  max {st['max_dt']:.3f} ms  (n={n} deltas)"
    )
    lines.append(
        f"  Implied rate (Pico) : mean {mean_hz:.1f} Hz  "
        f"instantaneous range ~{lo_hz:.1f}–{hi_hz:.1f} Hz (from dt max/min)"
    )
    lines.append("  (Use Pico timing for firmware cadence; PC rate includes USB/OS jitter.)")
    lines.append("=== End bench ===")
    return "\n".join(lines)


# ---------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------
RECONNECT_DELAY_S = 5
MAX_RECONNECTS    = 12


def soft_reset(ser):
    """
    Interrupt a running MicroPython script and trigger a soft reset so
    main.py reruns from the top.
    """
    print("[PC]  Draining Pico output and sending interrupt...")
    ser.timeout = 0.05
    ctrl_c_sent = False
    t0 = time.monotonic()
    while time.monotonic() - t0 < 2.0:
        ser.read(max(ser.in_waiting, 1))
        if not ctrl_c_sent and time.monotonic() - t0 > 1.0:
            ser.write(b'\x03\x03')
            ctrl_c_sent = True

    ser.reset_input_buffer()
    ser.write(b'\x04')
    ser.timeout = SERIAL_TIMEOUT_S
    print("[PC]  Soft-reset sent — waiting for Pico startup...")


def connect(port):
    ser = serial.Serial(port, BAUD_RATE, timeout=SERIAL_TIMEOUT_S, write_timeout=2)
    time.sleep(0.5)
    soft_reset(ser)
    latency_ms = handle_startup(ser)
    return ser, latency_ms


def resolve_port(port_arg=None):
    auto_port = find_port()
    if port_arg:
        if auto_port and auto_port != port_arg:
            print(f"Auto-detected port: {auto_port}")
            print(f"Using CLI override: {port_arg}")
        else:
            print(f"Using CLI port: {port_arg}")
        return port_arg
    if auto_port:
        print(f"Using auto-detected port: {auto_port}")
        return auto_port
    print("No serial port auto-detected. Try: python3 -u pclogger.py --list-ports")
    print("Then: python3 -u pclogger.py /dev/ttyACM0   (or your device path)")
    sys.exit(1)


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------
def main():
    args = parse_args()
    if args.list_ports:
        print_serial_ports()
        return
    port = resolve_port(args.port)
    marker_logger = MarkerLogger(
        args.markers_file,
        enabled=(not args.no_markers) and (args.bench is None),
    )

    frame_count     = 0
    crc_errors      = 0
    reconnect_count = 0
    t_start         = time.monotonic()
    t_last_report   = t_start
    t_next_suspicious_warn = t_start
    pico_state = {"prev_ts_ms": None, "pico_epoch": 0}

    bench_stats = bench_stats_init()
    bench_prev_pico = None
    bench_wall_s = None
    report_interval = BENCH_REPORT_INTERVAL_S if args.bench else REPORT_INTERVAL_S

    # Both writers opened before any data arrives so no frames are ever missed
    if args.bench and args.bench_no_disk:
        parquet_writer = NullParquetWriter()
        csv_writer     = NullCsvWriter()
    else:
        if PYARROW_AVAILABLE:
            parquet_writer = ParquetWriter(PARQUET_FILE, SCHEMA, FLUSH_EVERY)
        else:
            print("[PC]  pyarrow not available — CSV-only mode (Parquet disabled).")
            parquet_writer = NullParquetWriter()
        csv_writer     = CsvWriter(CSV_FILE)

    # ---------------------------------------------------------------
    # Cleanup — guarded against double-call (e.g. SIGTERM during KeyboardInterrupt)
    # ---------------------------------------------------------------
    _cleanup_called = False

    def cleanup(reason="exit"):
        nonlocal _cleanup_called
        if _cleanup_called:
            return
        _cleanup_called = True

        elapsed = time.monotonic() - t_start
        rate    = frame_count / elapsed if elapsed > 0 else 0
        marker_logger.add(f"logging_stopped:{reason}", source="auto")
        print(f"\n[{reason}]")
        print(f"  Frames received  : {frame_count}")
        print(f"  CRC errors       : {crc_errors}")
        print(f"  Reconnects       : {reconnect_count}")
        print(f"  Average rate     : {rate:.1f} Hz")
        print(f"  Duration         : {elapsed:.1f}s")
        if args.bench is not None and bench_wall_s is not None:
            print(format_bench_summary(bench_stats, frame_count, bench_wall_s, crc_errors, reconnect_count))
        # Close Parquet first (writes footer if process is healthy)
        parquet_writer.close()
        # Close CSV (flush + close; already contains all data even on crash)
        csv_writer.close()
        skip_disk = args.bench and args.bench_no_disk
        if not skip_disk and os.path.exists(CSV_FILE):
            csv_kb = os.path.getsize(CSV_FILE) / 1024
            print(f"  CSV written      : {CSV_FILE}  ({csv_kb:.1f} KB)")
        if not skip_disk and os.path.exists(PARQUET_FILE):
            pq_kb = os.path.getsize(PARQUET_FILE) / 1024
            print(f"  Parquet written  : {PARQUET_FILE}  ({pq_kb:.1f} KB)")
        print("Done.")

    signal.signal(signal.SIGTERM, lambda s, f: (cleanup("SIGTERM"), sys.exit(0)))
    if hasattr(signal, 'SIGHUP'):
        signal.signal(signal.SIGHUP, lambda s, f: (cleanup("SIGHUP"), sys.exit(0)))

    if args.bench is None:
        print(f"Logging to : {PARQUET_FILE}  (Parquet, flushed every {FLUSH_EVERY} frames)")
        print(f"           : {CSV_FILE}  (CSV, crash-safe, written every frame)")
        print(f"{'Timestamp':<27} {'Name':<10} {'Voltage':>9} "
              f"{'Current':>10} {'Power':>10} {'Status'}")
        print("-" * 80)
        marker_logger.add("logging_started", source="auto")
    else:
        print(f"Bench: {args.bench:g} s of frames after DATA_START (Ctrl+C to stop early)")
        if args.bench_no_disk:
            print("  --bench-no-disk: CSV/Parquet writes disabled")

    _exit_reason = "NormalExit"  # overwritten by exception handlers and early-return paths
    ser = None
    bench_done = False
    try:
        while True:
            consecutive_failures = 0
            while True:
                marker_logger.poll()
                try:
                    print(f"\nConnecting to {port}..." if reconnect_count == 0
                          else f"\nReconnecting to {port} "
                               f"(attempt {consecutive_failures + 1}/{MAX_RECONNECTS})...")
                    ser, latency_ms = connect(port)
                    print(f"Connected. Latency: {latency_ms:.2f} ms\n")
                    pico_state = {"prev_ts_ms": None, "pico_epoch": 0}
                    consecutive_failures = 0
                    bench_prev_pico = None
                    break

                except (serial.SerialException, RuntimeError, OSError) as e:
                    consecutive_failures += 1
                    reconnect_count      += 1
                    parquet_writer.flush_if_pending()
                    # csv_writer already has everything — no action needed
                    print(f"  Connection failed: {e}")
                    if consecutive_failures >= MAX_RECONNECTS:
                        print(f"  Giving up after {MAX_RECONNECTS} attempts.")
                        _exit_reason = "MaxReconnects"
                        return  # finally block calls cleanup()
                    print(f"  Retrying in {RECONNECT_DELAY_S}s...")
                    time.sleep(RECONNECT_DELAY_S)

            buf = bytearray()
            t_loop_start = time.monotonic()
            t_bench_deadline = (time.monotonic() + args.bench) if args.bench is not None else None

            try:
                while True:
                    if (
                        args.bench is not None
                        and t_bench_deadline is not None
                        and time.monotonic() >= t_bench_deadline
                    ):
                        bench_wall_s = time.monotonic() - t_loop_start
                        bench_done = True
                        break
                    marker_logger.poll()
                    chunk = ser.read(ser.in_waiting or MAX_READ_CHUNK)
                    if not chunk:
                        continue
                    buf.extend(chunk)

                    if len(buf) > BUF_MAX_BYTES:
                        print(f"[WARN] buffer exceeded {BUF_MAX_BYTES} bytes; resetting parser buffer")
                        buf = bytearray()
                        continue

                    while True:
                        magic_pos = buf.find(FRAME_MAGIC)
                        if magic_pos == -1:
                            if len(buf) > 1:
                                buf = buf[-1:]
                            break

                        min_hdr = magic_pos + len(FRAME_MAGIC) + FRAME_HDR_SZ
                        if len(buf) < min_hdr:
                            break

                        hdr_start    = magic_pos + len(FRAME_MAGIC)
                        _, n_sensors = struct.unpack_from(FRAME_HDR_FMT, buf, hdr_start)
                        frame_size   = len(FRAME_MAGIC) + FRAME_HDR_SZ + n_sensors * SENSOR_SZ + 1

                        if len(buf) - magic_pos < frame_size:
                            break

                        payload_start = magic_pos + len(FRAME_MAGIC)
                        frame_end     = magic_pos + frame_size
                        frame_data    = bytes(buf[payload_start:frame_end])

                        # PC time: timestamp_iso (wall) + pc_mono_ns (perf_counter_ns) — same instant.
                        # pc_mono_ns aligns with other logs on this host that use perf_counter_ns (NTP-safe).
                        # NOTE: frames from one ser.read() batch can share nearly identical stamps;
                        # do not use timestamp_iso for inter-frame intervals — use pico_ms_ext.
                        frame_arrival_ts, frame_mono_ns = now_iso_and_mono()
                        rows = parse_frame(frame_data, frame_arrival_ts, pico_state, frame_mono_ns)
                        if rows is None:
                            crc_errors += 1
                            # Advance by 1 byte only so the parser can resync to the
                            # next valid FRAME_MAGIC hiding inside the corrupt payload.
                            # Do NOT consume frame_end bytes — they may contain valid frames.
                            del buf[:magic_pos + 1]
                            continue

                        # CRC passed — safe to consume the full frame
                        del buf[:frame_end]

                        now = time.monotonic()
                        if now >= t_next_suspicious_warn:
                            for row in rows:
                                v = row["voltage_V"]
                                c = row["current_mA"]
                                if v <= 0.0 or v >= 30.0 or c < 0.0:
                                    print(
                                        f"[WARN] suspicious reading: "
                                        f"{row['name']} {v:.3f}V {c:.1f}mA "
                                        f"status={row['status']}"
                                    )
                                    t_next_suspicious_warn = now + SUSPICIOUS_WARN_INTERVAL_S
                                    break

                        frame_count += 1
                        if args.bench is not None and rows:
                            pext = rows[0]["pico_ms_ext"]
                            if bench_prev_pico is not None:
                                bench_stats_add(bench_stats, pext - bench_prev_pico)
                            bench_prev_pico = pext
                        parquet_writer.append(rows)   # efficient columnar, periodic flush
                        csv_writer.append(rows)        # crash-safe, every frame

                        if SAMPLE_PRINT_EVERY > 0 and frame_count % SAMPLE_PRINT_EVERY == 0:
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
                        if now - t_last_report >= report_interval:
                            elapsed = now - t_start
                            rate    = frame_count / elapsed
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

            if bench_done:
                _exit_reason = "BenchComplete"
                break

    except KeyboardInterrupt:
        _exit_reason = "KeyboardInterrupt"
    except Exception as e:
        # Unexpected exception — still save data before re-raising
        _exit_reason = f"UnexpectedException:{type(e).__name__}"
        print(f"\n[ERROR] Unexpected exception: {e}")
        raise
    finally:
        if ser and ser.is_open:
            try:
                ser.close()
            except Exception:
                pass
        cleanup(_exit_reason)  # always runs: writes Parquet footer + closes CSV


if __name__ == "__main__":
    main()
