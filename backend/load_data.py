#!/usr/bin/env python3
"""
Load CSV datasets into the database (PostgreSQL or SQLite).
"""

import csv
import os

from werkzeug.security import generate_password_hash

import db

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _integrity_error():
    if db.using_postgres():
        import psycopg2
        return psycopg2.IntegrityError
    else:
        import sqlite3
        return sqlite3.IntegrityError


def load_merchants():
    """Load merchants from CSV."""
    conn = db.get_db_connection()
    count = 0
    password_method = os.getenv('PASSWORD_HASH_METHOD', 'pbkdf2:sha256')

    csv_path = os.path.join(SCRIPT_DIR, 'merchants.csv')
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                raw_password = row.get('password')
                existing_hash = row.get('password_hash')

                if raw_password:
                    password_hash = generate_password_hash(raw_password, method=password_method)
                else:
                    password_hash = existing_hash

                if db.using_postgres():
                    db.execute(conn,
                        'INSERT INTO merchants (id, name, email, password_hash) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING',
                        (row['id'], row['name'], row['email'], password_hash))
                else:
                    db.execute(conn,
                        'INSERT OR IGNORE INTO merchants (id, name, email, password_hash) VALUES (?, ?, ?, ?)',
                        (row['id'], row['name'], row['email'], password_hash))
                count += 1
            except _integrity_error():
                print(f"  Skipping duplicate merchant: {row['name']}")

    # Reset PostgreSQL sequence to max id
    if db.using_postgres():
        db.execute(conn, "SELECT setval('merchants_id_seq', (SELECT COALESCE(MAX(id), 1) FROM merchants))")

    conn.commit()
    conn.close()
    print(f"Loaded {count} merchants")
    return count

def load_drivers():
    """Load drivers from CSV."""
    conn = db.get_db_connection()
    count = 0

    csv_path = os.path.join(SCRIPT_DIR, 'drivers.csv')
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if db.using_postgres():
                    db.execute(conn,
                        'INSERT INTO drivers (id, name) VALUES (?, ?) ON CONFLICT DO NOTHING',
                        (row['id'], row['name']))
                else:
                    db.execute(conn,
                        'INSERT OR IGNORE INTO drivers (id, name) VALUES (?, ?)',
                        (row['id'], row['name']))
                count += 1
            except _integrity_error():
                print(f"  Skipping duplicate driver: {row['name']}")

    if db.using_postgres():
        db.execute(conn, "SELECT setval('drivers_id_seq', (SELECT COALESCE(MAX(id), 1) FROM drivers))")

    conn.commit()
    conn.close()
    print(f"Loaded {count} drivers")
    return count

def load_vehicles():
    """Load vehicles from CSV."""
    conn = db.get_db_connection()
    count = 0

    csv_path = os.path.join(SCRIPT_DIR, 'vehicles.csv')
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if db.using_postgres():
                    db.execute(conn,
                        'INSERT INTO vehicles (id, driver_id, max_orders, max_weight) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING',
                        (row['id'], row['driver_id'], row['max_orders'], row['max_weight']))
                else:
                    db.execute(conn,
                        'INSERT OR IGNORE INTO vehicles (id, driver_id, max_orders, max_weight) VALUES (?, ?, ?, ?)',
                        (row['id'], row['driver_id'], row['max_orders'], row['max_weight']))
                count += 1
            except _integrity_error():
                print(f"  Skipping duplicate vehicle: {row['id']}")

    if db.using_postgres():
        db.execute(conn, "SELECT setval('vehicles_id_seq', (SELECT COALESCE(MAX(id), 1) FROM vehicles))")

    conn.commit()
    conn.close()
    print(f"Loaded {count} vehicles")
    return count

def load_shifts():
    """Load shifts from CSV."""
    conn = db.get_db_connection()
    count = 0

    csv_path = os.path.join(SCRIPT_DIR, 'shifts.csv')
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if db.using_postgres():
                    db.execute(conn,
                        'INSERT INTO shifts (id, driver_id, shift_date, start_time, end_time) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING',
                        (row['id'], row['driver_id'], row['shift_date'], row['start_time'], row['end_time']))
                else:
                    db.execute(conn,
                        'INSERT OR IGNORE INTO shifts (id, driver_id, shift_date, start_time, end_time) VALUES (?, ?, ?, ?, ?)',
                        (row['id'], row['driver_id'], row['shift_date'], row['start_time'], row['end_time']))
                count += 1
            except _integrity_error():
                print(f"  Skipping duplicate shift: driver {row['driver_id']} on {row['shift_date']}")

    if db.using_postgres():
        db.execute(conn, "SELECT setval('shifts_id_seq', (SELECT COALESCE(MAX(id), 1) FROM shifts))")

    conn.commit()
    conn.close()
    print(f"Loaded {count} shifts")
    return count

def load_orders():
    """Load orders from CSV."""
    conn = db.get_db_connection()
    count = 0

    csv_path = os.path.join(SCRIPT_DIR, 'orders.csv')
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                driver_id = row['driver_id'] if row['driver_id'] else None
                vehicle_id = row['vehicle_id'] if row['vehicle_id'] else None
                description = row.get('description', '')

                if db.using_postgres():
                    db.execute(conn,
                        '''INSERT INTO orders (id, merchant_id, driver_id, vehicle_id, status, description, pickup_time, dropoff_time, weight)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT (id) DO UPDATE SET
                             merchant_id = EXCLUDED.merchant_id,
                             driver_id = EXCLUDED.driver_id,
                             vehicle_id = EXCLUDED.vehicle_id,
                             status = EXCLUDED.status,
                             description = EXCLUDED.description,
                             pickup_time = EXCLUDED.pickup_time,
                             dropoff_time = EXCLUDED.dropoff_time,
                             weight = EXCLUDED.weight''',
                        (row['id'], row['merchant_id'], driver_id, vehicle_id, row['status'],
                         description, row['pickup_time'], row['dropoff_time'], row['weight']))
                else:
                    db.execute(conn,
                        'INSERT OR REPLACE INTO orders (id, merchant_id, driver_id, vehicle_id, status, description, pickup_time, dropoff_time, weight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                        (row['id'], row['merchant_id'], driver_id, vehicle_id, row['status'],
                         description, row['pickup_time'], row['dropoff_time'], row['weight']))
                count += 1
            except _integrity_error():
                print(f"  Skipping duplicate order: {row['id']}")
            except Exception as e:
                print(f"  Error loading order {row['id']}: {e}")

    if db.using_postgres():
        db.execute(conn, "SELECT setval('orders_id_seq', (SELECT COALESCE(MAX(id), 1) FROM orders))")

    conn.commit()
    conn.close()
    print(f"Loaded {count} orders")
    return count

def assign_pending_orders():
    """Run driver assignment for all pending orders after CSV import."""
    from orders_service import assign_driver_to_order

    conn = db.get_db_connection()

    pending_orders = db.fetchall(db.execute(conn, '''
        SELECT id, pickup_time, dropoff_time, weight
        FROM orders
        WHERE status = 'pending'
        ORDER BY id
    '''))

    print(f"\nProcessing {len(pending_orders)} pending orders for driver assignment...")

    assigned = 0
    for order in pending_orders:
        pickup_str = order['pickup_time']
        dropoff_str = order['dropoff_time']
        # PostgreSQL returns datetime objects; convert to string
        if not isinstance(pickup_str, str):
            pickup_str = pickup_str.isoformat()
        if not isinstance(dropoff_str, str):
            dropoff_str = dropoff_str.isoformat()

        driver_id, vehicle_id = assign_driver_to_order(
            conn,
            order['id'],
            pickup_str,
            dropoff_str,
            order['weight']
        )
        if driver_id and vehicle_id:
            assigned += 1

    conn.close()
    print(f"Assigned {assigned} orders to drivers")
    print(f"{len(pending_orders) - assigned} orders remain pending (no available driver/vehicle)")

    return assigned, len(pending_orders) - assigned

if __name__ == '__main__':
    print("Loading CSV data into database...")
    print("=" * 50)

    if not db.using_postgres():
        DATABASE_PATH = db.DATABASE_PATH
        os.makedirs(os.path.dirname(DATABASE_PATH) if os.path.dirname(DATABASE_PATH) else '.', exist_ok=True)

    required_files = ['merchants.csv', 'drivers.csv', 'vehicles.csv', 'shifts.csv', 'orders.csv']
    missing_files = [f for f in required_files if not os.path.exists(os.path.join(SCRIPT_DIR, f))]

    if missing_files:
        print(f"Error: Missing CSV files: {', '.join(missing_files)}")
        print("Please run generate_datasets.py first to create the CSV files.")
        exit(1)

    from app import init_db
    init_db()
    print(f"Database schema initialized ({'PostgreSQL' if db.using_postgres() else 'SQLite'})\n")

    merchants_count = load_merchants()
    drivers_count = load_drivers()
    vehicles_count = load_vehicles()
    shifts_count = load_shifts()
    orders_count = load_orders()

    assigned_count, still_pending_count = assign_pending_orders()

    print("=" * 50)
    print("Data loading complete!")
    print(f"\nSummary:")
    print(f"  - {merchants_count} merchants")
    print(f"  - {drivers_count} drivers")
    print(f"  - {vehicles_count} vehicles")
    print(f"  - {shifts_count} shifts")
    print(f"  - {orders_count} orders loaded")
    print(f"  - {assigned_count} orders assigned to drivers")
    print(f"  - {still_pending_count} orders still pending")
