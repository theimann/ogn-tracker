#!/usr/bin/env python3
"""
OGN Device Database (DDB) Sync
Downloads the OGN DDB CSV and upserts into MariaDB.
Run daily via cron/systemd timer.

Table: ogn_ddb
  - device_type: F=FLARM, O=OGN, I=ICAO
  - device_id: hex ID (e.g. 'DD1234')
  - aircraft_model: e.g. 'ASW 24', 'LS-8'
  - registration: e.g. 'D-KXYZ'
  - cn: competition number
  - tracked: Y/N (owner allows tracking)
  - identified: Y/N (owner allows identification)
  - aircraft_type: 1=glider, 2=tow, 3=heli, etc.
"""

import os
import sys
import csv
import io
import logging
import urllib.request
from datetime import datetime

import mysql.connector

# --- Configuration ---
DDB_URL = 'http://ddb.glidernet.org/download/?t=1'

# Load .env
ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
env = {}
if os.path.exists(ENV_FILE):
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()

DB_HOST = env.get('DB_HOST', 'localhost')
DB_USER = env.get('DB_USER', 'ogn_collector')
DB_PASS = env.get('DB_PASS', '')
DB_NAME = env.get('DB_NAME', 'ogn')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger('ddb-sync')


def download_ddb():
    """Download OGN DDB CSV and return parsed rows."""
    log.info(f"Downloading DDB from {DDB_URL}")
    req = urllib.request.Request(DDB_URL, headers={'User-Agent': 'ogn-collector/1.0'})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode('utf-8')

    rows = []
    reader = csv.reader(io.StringIO(raw))
    for line in reader:
        # Skip comments/headers
        if not line or line[0].startswith('#'):
            continue
        if len(line) < 8:
            continue

        # Strip quotes from CSV values
        vals = [v.strip().strip("'") for v in line]
        rows.append({
            'device_type': vals[0],
            'device_id': vals[1].upper(),
            'aircraft_model': vals[2],
            'registration': vals[3],
            'cn': vals[4],
            'tracked': vals[5],
            'identified': vals[6],
            'aircraft_type': int(vals[7]) if vals[7].isdigit() else None,
        })

    log.info(f"Downloaded {len(rows)} devices")
    return rows


def init_table(conn):
    """Create ogn_ddb table if not exists."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ogn_ddb (
            device_type CHAR(1) NOT NULL COMMENT 'F=FLARM, O=OGN, I=ICAO',
            device_id VARCHAR(20) NOT NULL,
            aircraft_model VARCHAR(100) DEFAULT '',
            registration VARCHAR(20) DEFAULT '',
            cn VARCHAR(10) DEFAULT '' COMMENT 'Competition number',
            tracked CHAR(1) DEFAULT 'Y' COMMENT 'Y/N owner allows tracking',
            identified CHAR(1) DEFAULT 'Y' COMMENT 'Y/N owner allows identification',
            aircraft_type TINYINT UNSIGNED NULL COMMENT '1=glider 2=tow 3=heli etc',
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (device_type, device_id),
            INDEX idx_registration (registration),
            INDEX idx_device_id (device_id),
            INDEX idx_aircraft_model (aircraft_model)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cursor.close()


def sync_ddb(conn, rows):
    """Upsert DDB rows into MariaDB."""
    cursor = conn.cursor()
    
    upsert_sql = """
        INSERT INTO ogn_ddb (device_type, device_id, aircraft_model, registration, cn, tracked, identified, aircraft_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            aircraft_model = VALUES(aircraft_model),
            registration = VALUES(registration),
            cn = VALUES(cn),
            tracked = VALUES(tracked),
            identified = VALUES(identified),
            aircraft_type = VALUES(aircraft_type),
            updated_at = CURRENT_TIMESTAMP
    """

    batch = []
    for r in rows:
        batch.append((
            r['device_type'], r['device_id'], r['aircraft_model'],
            r['registration'], r['cn'], r['tracked'], r['identified'],
            r['aircraft_type']
        ))

        if len(batch) >= 500:
            cursor.executemany(upsert_sql, batch)
            conn.commit()
            batch = []

    if batch:
        cursor.executemany(upsert_sql, batch)
        conn.commit()

    # Count final rows
    cursor.execute("SELECT COUNT(*) FROM ogn_ddb")
    total = cursor.fetchone()[0]
    cursor.close()
    
    log.info(f"Synced {len(rows)} devices, {total} total in database")
    return total


def main():
    try:
        rows = download_ddb()
    except Exception as e:
        log.error(f"Download failed: {e}")
        sys.exit(1)

    try:
        conn = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
        )
        init_table(conn)
        total = sync_ddb(conn, rows)
        conn.close()
        log.info(f"DDB sync complete: {total} devices")
    except Exception as e:
        log.error(f"Database error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
