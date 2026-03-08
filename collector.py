#!/usr/bin/env python3
"""
OGN Data Collector
Connects to the Open Glider Network APRS stream and stores:
  1. Raw APRS messages in MariaDB (ogn_raw table)
  2. Parsed position data in MariaDB (ogn_positions table)
  3. Position metrics to InfluxDB on j4llanas
"""

import os
import sys
import json
import time
import signal
import logging
from datetime import datetime, timezone
from threading import Thread, Event
from queue import Queue, Empty

from ogn.parser import parse, AprsParseError

# InfluxDB v1
from influxdb import InfluxDBClient

# MariaDB
import mysql.connector
from mysql.connector import pooling

# --- Configuration ---
APRS_USER = os.environ.get('OGN_APRS_USER', 'gliderincidents')
# Filter: 500km radius around central Europe (covers Alps, Germany, Switzerland, Austria, France)
APRS_FILTER = os.environ.get('OGN_APRS_FILTER', 'r/47.5/11.5/500')

DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_USER = os.environ.get('DB_USER', 'ogn_collector')
DB_PASS = os.environ.get('DB_PASS', '')
DB_NAME = os.environ.get('DB_NAME', 'ogn')

INFLUX_HOST = os.environ.get('INFLUX_HOST', '100.89.150.28')
INFLUX_PORT = int(os.environ.get('INFLUX_PORT', '8086'))
INFLUX_DB = os.environ.get('INFLUX_DB', 'ogn')

# Batch settings
BATCH_SIZE = 100
FLUSH_INTERVAL = 5  # seconds

# Aircraft types to store (OGN protocol):
# 0=Unknown, 1=Glider/Motorglider, 2=Tow plane, 3=Helicopter,
# 7=Paraglider, 8=Powered small, 9=Jet, 10=UFO, 13=UAV, 14=Static, 15=Emergency
# We skip: 9 (jets/airlines) — that's ~85% of traffic
STORE_AIRCRAFT_TYPES = {1, 2}  # 1=Glider/Motorglider, 2=Tow plane
SKIP_AIRCRAFT_TYPES = None  # Not used — we whitelist instead

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# --- Logging ---
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('ogn-collector')

# --- Globals ---
shutdown_event = Event()
msg_queue = Queue(maxsize=50000)
stats = {
    'received': 0,
    'parsed': 0,
    'parse_errors': 0,
    'skipped': 0,
    'db_writes': 0,
    'influx_writes': 0,
    'db_errors': 0,
    'influx_errors': 0,
    'started_at': None,
}


def signal_handler(signum, frame):
    log.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()


def init_mariadb():
    """Create database and tables if they don't exist."""
    conn = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )
    cursor = conn.cursor()

    # Parsed positions table (structured, queryable)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ogn_positions (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            received_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            timestamp DATETIME NULL,
            device_id VARCHAR(20) NULL,
            device_type VARCHAR(10) NULL COMMENT 'FLARM, OGN, ICAO, etc',
            aprs_type VARCHAR(20) NULL,
            latitude DECIMAL(10,7) NULL,
            longitude DECIMAL(11,7) NULL,
            altitude_m INT NULL,
            ground_speed_kph DECIMAL(6,1) NULL,
            track_deg SMALLINT UNSIGNED NULL,
            climb_rate_ms DECIMAL(6,2) NULL,
            turn_rate_dps DECIMAL(5,1) NULL,
            signal_db DECIMAL(4,1) NULL,
            frequency_offset_khz DECIMAL(5,1) NULL,
            gps_accuracy VARCHAR(10) NULL,
            receiver VARCHAR(30) NULL,
            aircraft_type TINYINT UNSIGNED NULL,
            INDEX idx_received (received_at),
            INDEX idx_device (device_id, received_at),
            INDEX idx_position (latitude, longitude),
            INDEX idx_timestamp (timestamp)
        ) ENGINE=InnoDB
    """)

    conn.commit()
    cursor.close()
    conn.close()
    log.info("MariaDB tables initialized")


def get_db_pool():
    """Create a connection pool for MariaDB."""
    return pooling.MySQLConnectionPool(
        pool_name="ogn_pool",
        pool_size=3,
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=False,
        time_zone='+00:00',
    )


def get_influx():
    """Create InfluxDB client."""
    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)
    # Create database if not exists
    client.create_database(INFLUX_DB)
    return client


def process_beacon(raw_message):
    """Callback for each APRS message from OGN."""
    if shutdown_event.is_set():
        return

    stats['received'] += 1

    # Skip server comments
    if raw_message.startswith('#'):
        return

    try:
        msg_queue.put_nowait((raw_message, datetime.now(timezone.utc)))
    except Exception:
        pass  # Drop message if queue is full


def writer_thread(db_pool, influx):
    """Background thread that batches writes to MariaDB and InfluxDB."""
    pos_batch = []
    influx_batch = []
    last_flush = time.time()
    last_stats = time.time()

    while not shutdown_event.is_set():
        try:
            raw_msg, received_at = msg_queue.get(timeout=1)
        except Empty:
            # Flush on timeout if there's pending data
            if pos_batch:
                flush_batches(db_pool, influx, pos_batch, influx_batch)
                pos_batch, influx_batch = [], []
                last_flush = time.time()
            continue

        now_str = received_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Try to parse
        try:
            beacon = parse(raw_msg)
            stats['parsed'] += 1

            # Only store gliders (1) and tow planes (2)
            aircraft_type = beacon.get('aircraft_type')
            if aircraft_type not in STORE_AIRCRAFT_TYPES:
                stats['skipped'] += 1
                continue

            if beacon.get('latitude') and beacon.get('longitude'):
                pos_batch.append(extract_position(beacon, now_str))
                influx_batch.append(build_influx_point(beacon, received_at))

        except (AprsParseError, Exception) as e:
            stats['parse_errors'] += 1

        # Flush if batch is full or interval elapsed
        if len(pos_batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_INTERVAL:
            flush_batches(db_pool, influx, pos_batch, influx_batch)
            pos_batch, influx_batch = [], []
            last_flush = time.time()

        # Log stats every 60 seconds
        if time.time() - last_stats >= 60:
            elapsed = time.time() - stats['started_at']
            log.info(
                f"Stats: {stats['received']} recv, {stats['parsed']} parsed, "
                f"{stats['skipped']} skipped (jets), {stats['db_writes']} DB, "
                f"{stats['influx_writes']} InfluxDB, "
                f"{stats['received']/elapsed:.1f} msg/s, queue={msg_queue.qsize()}"
            )
            last_stats = time.time()

    # Final flush
    if pos_batch:
        flush_batches(db_pool, influx, pos_batch, influx_batch)


def extract_position(beacon, now_str):
    """Extract structured position data from parsed beacon."""
    # Convert altitude from feet to meters
    alt_m = None
    if beacon.get('altitude'):
        alt_m = int(beacon['altitude'] * 0.3048)

    # Convert climb rate from fpm to m/s
    climb_ms = None
    if beacon.get('climb_rate') is not None:
        climb_ms = round(beacon['climb_rate'] * 0.00508, 2)

    # Ground speed: already in km/h in ogn-parser
    speed = beacon.get('ground_speed')

    return (
        now_str,
        beacon.get('timestamp', '').strftime('%Y-%m-%d %H:%M:%S') if hasattr(beacon.get('timestamp', ''), 'strftime') else None,
        beacon.get('address'),
        beacon.get('address_type'),
        beacon.get('aprs_type'),
        beacon.get('latitude'),
        beacon.get('longitude'),
        alt_m,
        speed,
        beacon.get('track'),
        climb_ms,
        beacon.get('turn_rate'),
        beacon.get('signal_quality'),
        beacon.get('frequency_offset'),
        beacon.get('gps_quality'),
        beacon.get('name'),  # receiver
        beacon.get('aircraft_type'),
    )


def build_influx_point(beacon, received_at):
    """Build an InfluxDB point from parsed beacon."""
    tags = {
        'device_id': beacon.get('address', 'unknown'),
        'device_type': beacon.get('address_type', 'unknown'),
        'receiver': beacon.get('name', 'unknown'),
    }
    if beacon.get('aircraft_type') is not None:
        tags['aircraft_type'] = str(beacon['aircraft_type'])

    fields = {}
    if beacon.get('latitude'):
        fields['latitude'] = float(beacon['latitude'])
    if beacon.get('longitude'):
        fields['longitude'] = float(beacon['longitude'])
    if beacon.get('altitude'):
        fields['altitude_m'] = int(beacon['altitude'] * 0.3048)
    if beacon.get('ground_speed') is not None:
        fields['speed_kph'] = float(beacon['ground_speed'])
    if beacon.get('track') is not None:
        fields['track_deg'] = int(beacon['track'])
    if beacon.get('climb_rate') is not None:
        fields['climb_rate_ms'] = round(beacon['climb_rate'] * 0.00508, 2)
    if beacon.get('turn_rate') is not None:
        fields['turn_rate_dps'] = float(beacon['turn_rate'])
    if beacon.get('signal_quality') is not None:
        fields['signal_db'] = float(beacon['signal_quality'])

    if not fields:
        return None

    return {
        'measurement': 'ogn_position',
        'tags': tags,
        'time': received_at.isoformat(),
        'fields': fields,
    }


def flush_batches(db_pool, influx, pos_batch, influx_batch):
    """Write batches to MariaDB and InfluxDB."""
    # MariaDB: parsed positions
    if pos_batch:
        conn = None
        try:
            conn = db_pool.get_connection()
            cursor = conn.cursor()
            cursor.executemany(
                """INSERT INTO ogn_positions 
                   (received_at, timestamp, device_id, device_type, aprs_type,
                    latitude, longitude, altitude_m, ground_speed_kph, track_deg,
                    climb_rate_ms, turn_rate_dps, signal_db, frequency_offset_khz,
                    gps_accuracy, receiver, aircraft_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                pos_batch
            )
            conn.commit()
            stats['db_writes'] += len(pos_batch)
            cursor.close()
        except Exception as e:
            stats['db_errors'] += 1
            log.error(f"MariaDB positions write error: {e}")
        finally:
            if conn:
                conn.close()

    # InfluxDB (disabled)
    if influx:
        influx_points = [p for p in influx_batch if p is not None]
        if influx_points:
            try:
                influx.write_points(influx_points)
                stats['influx_writes'] += len(influx_points)
            except Exception as e:
                stats['influx_errors'] += 1
                log.error(f"InfluxDB write error: {e}")


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    log.info("OGN Collector starting...")
    log.info(f"APRS filter: {APRS_FILTER}")
    log.info(f"MariaDB: {DB_USER}@{DB_HOST}/{DB_NAME}")
    log.info(f"InfluxDB: {INFLUX_HOST}:{INFLUX_PORT}/{INFLUX_DB}")

    # Initialize
    init_mariadb()
    db_pool = get_db_pool()
    influx = None  # InfluxDB disabled for now
    # influx = get_influx()

    stats['started_at'] = time.time()

    # Start writer thread
    writer = Thread(target=writer_thread, args=(db_pool, influx), daemon=True)
    writer.start()

    # Connect to OGN APRS via raw TCP
    import socket as _socket

    APRS_HOST = 'aprs.glidernet.org'
    APRS_PORT = 14580

    while not shutdown_event.is_set():
        sock = None
        try:
            log.info(f"Connecting to {APRS_HOST}:{APRS_PORT}...")
            sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            sock.settimeout(60)
            sock.connect((APRS_HOST, APRS_PORT))

            # Login
            login_str = f"user {APRS_USER} pass -1 vers ogn-collector 1.0 filter {APRS_FILTER}\r\n"
            sock.send(login_str.encode())
            log.info(f"Connected! Filter: {APRS_FILTER}")

            sock_file = sock.makefile('rb')
            keepalive_time = time.time()

            while not shutdown_event.is_set():
                # Send keepalive every 4 minutes
                if time.time() - keepalive_time > 240:
                    sock.send(b'#keepalive\r\n')
                    keepalive_time = time.time()

                try:
                    line = sock_file.readline()
                    if not line:
                        log.warning("Connection closed by server")
                        break
                    msg = line.decode(errors='replace').strip()
                    if msg:
                        process_beacon(msg)
                except _socket.timeout:
                    # No data for 60s, send keepalive and continue
                    sock.send(b'#keepalive\r\n')
                    keepalive_time = time.time()
                    continue

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"APRS connection error: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

        if not shutdown_event.is_set():
            log.info("Reconnecting in 10 seconds...")
            time.sleep(10)

    log.info("Shutting down...")
    shutdown_event.set()
    writer.join(timeout=10)

    log.info(
        f"Final stats: {stats['received']} received, {stats['parsed']} parsed, "
        f"{stats['db_writes']} DB writes, {stats['influx_writes']} InfluxDB writes"
    )


if __name__ == '__main__':
    main()
