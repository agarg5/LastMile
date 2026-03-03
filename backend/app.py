import os
import threading
from datetime import datetime, timedelta, time as dt_time
from functools import wraps

import jwt
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.security import check_password_hash, generate_password_hash

import db
from db import DATABASE_PATH as DB_DATABASE_PATH
from orders_service import (
    validate_order_times,
    find_available_driver,
    assign_driver_to_order,
)
from websocket_service import register_socketio_handlers, start_location_updates

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Database path is defined in db.py but re-exported here so tests can
# monkeypatch app.DATABASE_PATH and have it affect all DB access.
DATABASE_PATH = DB_DATABASE_PATH


def get_db_connection():
    """Create a database connection using the current DATABASE_PATH.

    Tests patch app.DATABASE_PATH; keep db.DATABASE_PATH in sync so all
    helpers use the same database file.
    """
    db.DATABASE_PATH = DATABASE_PATH
    return db.get_db_connection()


def init_db():
    """Initialize the database using the current DATABASE_PATH."""
    db.DATABASE_PATH = DATABASE_PATH
    return db.init_db()


def _integrity_error():
    """Return the IntegrityError class for the active database backend."""
    if db.using_postgres():
        import psycopg2
        return psycopg2.IntegrityError
    else:
        import sqlite3
        return sqlite3.IntegrityError


# JWT configuration (simple symmetric HS256 token)
JWT_SECRET = os.getenv('JWT_SECRET', 'dev-secret-key-change-me')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION_MINUTES = int(os.getenv('JWT_EXPIRATION_MINUTES', '60'))
# Force a hashing algorithm that does not rely on hashlib.scrypt, which
# is missing in some Python builds (seen on certain macOS environments).
PASSWORD_HASH_METHOD = os.getenv('PASSWORD_HASH_METHOD', 'pbkdf2:sha256')

# Lock for order updates (to prevent race conditions)
order_locks = {}
lock_manager = threading.Lock()


def create_access_token(merchant_row):
    """Create a signed JWT for a merchant row."""
    now = datetime.utcnow()
    payload = {
        'sub': merchant_row['id'],
        'email': merchant_row['email'],
        'iat': now,
        'exp': now + timedelta(minutes=JWT_EXPIRATION_MINUTES),
        'type': 'access',
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    # PyJWT can return bytes in some versions; normalize to str
    if isinstance(token, bytes):
        token = token.decode('utf-8')
    return token


# ==================== DRIVERS ====================


@app.route('/drivers', methods=['GET'])
def get_drivers():
    """Retrieve drivers and their shifts."""
    conn = get_db_connection()
    drivers = db.fetchall(db.execute(conn, 'SELECT * FROM drivers ORDER BY id'))

    result = []
    for driver in drivers:
        driver_id = driver['id']
        shifts = db.fetchall(db.execute(conn, '''
            SELECT * FROM shifts
            WHERE driver_id = ?
            ORDER BY shift_date, start_time
        ''', (driver_id,)))

        driver_data = dict(driver)
        driver_data['shifts'] = shifts
        result.append(driver_data)

    conn.close()
    return jsonify(result)


# ==================== SHIFTS ====================

@app.route('/shifts', methods=['GET'])
def get_shifts():
    """Get all shifts."""
    conn = get_db_connection()
    shifts = db.fetchall(db.execute(conn, '''
        SELECT s.*, d.name as driver_name
        FROM shifts s
        LEFT JOIN drivers d ON s.driver_id = d.id
        ORDER BY s.shift_date, s.start_time
    '''))
    conn.close()
    return jsonify(shifts)


@app.route('/drivers', methods=['POST'])
def create_driver():
    """Create a new driver."""
    data = request.get_json() or {}
    name = data.get('name')

    if not name:
        return jsonify({"error": "Name is required"}), 400

    conn = get_db_connection()
    try:
        if db.using_postgres():
            cur = db.execute(conn,
                'INSERT INTO drivers (name) VALUES (?) RETURNING id',
                (name,))
            driver_id = db.fetchone(cur)['id']
        else:
            cur = db.execute(conn,
                'INSERT INTO drivers (name) VALUES (?)',
                (name,))
            driver_id = cur.lastrowid
        conn.commit()
        driver = db.fetchone(db.execute(conn,
            'SELECT * FROM drivers WHERE id = ?', (driver_id,)))
        conn.close()
        return jsonify(driver), 201
    except _integrity_error():
        conn.rollback()
        conn.close()
        return jsonify({"error": "Driver with this name already exists"}), 400


@app.route('/vehicles', methods=['POST'])
def create_vehicle():
    """Create a new vehicle."""
    data = request.get_json() or {}
    driver_id = data.get('driver_id')
    max_orders = data.get('max_orders')
    max_weight = data.get('max_weight')

    if not all([driver_id, max_orders, max_weight]):
        return jsonify({"error": "driver_id, max_orders, and max_weight are required"}), 400

    conn = get_db_connection()

    driver = db.fetchone(db.execute(conn,
        'SELECT * FROM drivers WHERE id = ?', (driver_id,)))
    if not driver:
        conn.close()
        return jsonify({"error": "Driver not found"}), 404

    try:
        if db.using_postgres():
            cur = db.execute(conn,
                'INSERT INTO vehicles (driver_id, max_orders, max_weight) VALUES (?, ?, ?) RETURNING id',
                (driver_id, max_orders, max_weight))
            vehicle_id = db.fetchone(cur)['id']
        else:
            cur = db.execute(conn,
                'INSERT INTO vehicles (driver_id, max_orders, max_weight) VALUES (?, ?, ?)',
                (driver_id, max_orders, max_weight))
            vehicle_id = cur.lastrowid
        conn.commit()
        vehicle = db.fetchone(db.execute(conn,
            'SELECT * FROM vehicles WHERE id = ?', (vehicle_id,)))
        conn.close()
        return jsonify(vehicle), 201
    except _integrity_error():
        conn.rollback()
        conn.close()
        return jsonify({"error": "Vehicle for this driver already exists"}), 400


@app.route('/shifts', methods=['POST'])
def create_shift():
    """Create a new shift for a driver."""
    data = request.get_json() or {}
    driver_id = data.get('driver_id')
    shift_date = data.get('shift_date')
    start_time = data.get('start_time')
    end_time = data.get('end_time')

    if not all([driver_id, shift_date, start_time, end_time]):
        return jsonify({"error": "driver_id, shift_date, start_time, and end_time are required"}), 400

    conn = get_db_connection()

    driver = db.fetchone(db.execute(conn,
        'SELECT * FROM drivers WHERE id = ?', (driver_id,)))
    if not driver:
        conn.close()
        return jsonify({"error": "Driver not found"}), 404

    try:
        if db.using_postgres():
            cur = db.execute(conn,
                'INSERT INTO shifts (driver_id, shift_date, start_time, end_time) VALUES (?, ?, ?, ?) RETURNING id',
                (driver_id, shift_date, start_time, end_time))
            shift_id = db.fetchone(cur)['id']
        else:
            cur = db.execute(conn,
                'INSERT INTO shifts (driver_id, shift_date, start_time, end_time) VALUES (?, ?, ?, ?)',
                (driver_id, shift_date, start_time, end_time))
            shift_id = cur.lastrowid
        conn.commit()
        shift = db.fetchone(db.execute(conn,
            'SELECT * FROM shifts WHERE id = ?', (shift_id,)))
        conn.close()
        return jsonify(shift), 201
    except _integrity_error():
        conn.rollback()
        conn.close()
        return jsonify({"error": "Shift for this driver and date already exists"}), 400


# ==================== ORDERS ====================


@app.route('/orders', methods=['GET'])
def get_orders():
    """Retrieve all orders for the merchant with pagination and search."""
    merchant_id = request.args.get('merchant_id', type=int)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    search = request.args.get('search', type=str)

    if not merchant_id:
        return jsonify({"error": "merchant_id query parameter is required"}), 400

    conn = get_db_connection()

    where_clause = "o.merchant_id = ?"
    params = [merchant_id]

    if search:
        where_clause += " AND (CAST(o.id AS TEXT) LIKE ? OR o.description LIKE ? OR d.name LIKE ?)"
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern, search_pattern])

    total_query = f'SELECT COUNT(*) as count FROM orders o LEFT JOIN drivers d ON o.driver_id = d.id WHERE {where_clause}'
    total = db.fetchone(db.execute(conn, total_query, tuple(params)))['count']

    offset = (page - 1) * per_page
    orders_query = f'''
        SELECT o.id as order_id, o.merchant_id, o.status, o.driver_id,
               o.description, o.pickup_time, o.dropoff_time, o.weight,
               d.name as driver_name
        FROM orders o
        LEFT JOIN drivers d ON o.driver_id = d.id
        WHERE {where_clause}
        ORDER BY o.created_at DESC
        LIMIT ? OFFSET ?
    '''
    orders = db.fetchall(db.execute(conn, orders_query, tuple(
        params + [per_page, offset])))

    conn.close()

    formatted_orders = []
    for order_dict in orders:
        formatted_order = {
            'order_id': order_dict['order_id'],
            'merchant_id': order_dict['merchant_id'],
            'status': order_dict['status'],
            'driver_id': order_dict['driver_id'],
            'description': order_dict['description'],
            'pickup_time': _serialize_timestamp(order_dict['pickup_time']),
            'dropoff_time': _serialize_timestamp(order_dict['dropoff_time']),
            'weight': order_dict['weight'],
        }
        if order_dict['driver_id'] and order_dict['driver_name']:
            formatted_order['driver'] = {
                'id': order_dict['driver_id'],
                'name': order_dict['driver_name']
            }
        formatted_orders.append(formatted_order)

    total_pages = (total + per_page - 1) // per_page if per_page else 1

    return jsonify({
        'orders': formatted_orders,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
    })


def _serialize_timestamp(value):
    """Ensure timestamp is returned as an ISO string (PostgreSQL returns datetime objects)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return value.isoformat()


@app.route('/orders', methods=['POST'])
def create_order():
    """Create a new order and assign it to a driver (if possible)."""
    data = request.get_json()
    merchant_id = data.get('merchant_id')
    description = data.get('description', '')
    pickup_time = data.get('pickup_time')
    dropoff_time = data.get('dropoff_time')
    weight = data.get('weight')

    if not all([merchant_id, pickup_time, dropoff_time, weight]):
        return jsonify({"error": "merchant_id, pickup_time, dropoff_time, and weight are required"}), 400

    valid, error_msg = validate_order_times(pickup_time, dropoff_time)
    if not valid:
        return jsonify({"error": error_msg}), 400

    conn = get_db_connection()

    merchant = db.fetchone(db.execute(conn,
        'SELECT * FROM merchants WHERE id = ?', (merchant_id,)))
    if not merchant:
        conn.close()
        return jsonify({"error": "Merchant not found"}), 404

    if db.using_postgres():
        cur = db.execute(conn,
            'INSERT INTO orders (merchant_id, description, status, pickup_time, dropoff_time, weight) VALUES (?, ?, ?, ?, ?, ?) RETURNING id',
            (merchant_id, description, 'pending', pickup_time, dropoff_time, weight))
        order_id = db.fetchone(cur)['id']
    else:
        cur = db.execute(conn,
            'INSERT INTO orders (merchant_id, description, status, pickup_time, dropoff_time, weight) VALUES (?, ?, ?, ?, ?, ?)',
            (merchant_id, description, 'pending', pickup_time, dropoff_time, weight))
        order_id = cur.lastrowid

    driver_id, vehicle_id = assign_driver_to_order(
        conn, order_id, pickup_time, dropoff_time, weight)

    order = db.fetchone(db.execute(conn, '''
        SELECT o.id as order_id, o.merchant_id, o.description, o.pickup_time,
               o.dropoff_time, o.weight, o.status, d.id as driver_id, d.name as driver_name
        FROM orders o
        LEFT JOIN drivers d ON o.driver_id = d.id
        WHERE o.id = ?
    ''', (order_id,)))

    conn.close()

    response = {
        "order_id": order['order_id'],
        "merchant_id": order['merchant_id'],
        "description": order['description'],
        "pickup_time": _serialize_timestamp(order['pickup_time']),
        "dropoff_time": _serialize_timestamp(order['dropoff_time']),
        "weight": order['weight'],
        "status": order['status']
    }

    if order['driver_id']:
        response["driver"] = {
            "id": order['driver_id'],
            "name": order['driver_name']
        }

    return jsonify(response), 201


@app.route('/orders/<int:order_id>', methods=['PUT'])
def update_order(order_id):
    """
    Modify an existing order.
    - Only the same merchant who created the order can edit it
    - Only one order can be edited at a time (locking)
    - Completed or cancelled orders cannot be edited
    - Re-run assignment logic if time or weight changes
    """
    data = request.get_json()
    merchant_id = data.get('merchant_id')

    if not merchant_id:
        return jsonify({"error": "merchant_id is required"}), 400

    with lock_manager:
        if order_id not in order_locks:
            order_locks[order_id] = threading.Lock()
        order_lock = order_locks[order_id]

    with order_lock:
        conn = get_db_connection()

        order = db.fetchone(db.execute(conn,
            'SELECT * FROM orders WHERE id = ?', (order_id,)))
        if not order:
            conn.close()
            return jsonify({"error": "Order not found"}), 404

        if order['merchant_id'] != merchant_id:
            conn.close()
            return jsonify({"error": "Only the merchant who created the order can edit it"}), 403

        if order['status'] in ['completed', 'cancelled']:
            conn.close()
            return jsonify({"error": "Cannot edit completed or cancelled orders"}), 400

        description = data.get('description', order['description'])
        pickup_time = data.get('pickup_time', _serialize_timestamp(order['pickup_time']))
        dropoff_time = data.get('dropoff_time', _serialize_timestamp(order['dropoff_time']))
        weight = data.get('weight', order['weight'])

        order_pickup_str = _serialize_timestamp(order['pickup_time'])
        order_dropoff_str = _serialize_timestamp(order['dropoff_time'])

        if pickup_time != order_pickup_str or dropoff_time != order_dropoff_str:
            valid, error_msg = validate_order_times(pickup_time, dropoff_time)
            if not valid:
                conn.close()
                return jsonify({"error": error_msg}), 400

        time_changed = (pickup_time != order_pickup_str or
                        dropoff_time != order_dropoff_str)
        weight_changed = weight != order['weight']
        needs_reassignment = time_changed or weight_changed

        db.execute(conn, '''
            UPDATE orders
            SET description = ?, pickup_time = ?, dropoff_time = ?, weight = ?
            WHERE id = ?
        ''', (description, pickup_time, dropoff_time, weight, order_id))

        if needs_reassignment:
            old_driver_id = order['driver_id']
            driver_id = None
            vehicle_id = None

            if old_driver_id:
                vehicle = db.fetchone(db.execute(conn,
                    'SELECT id, max_orders, max_weight FROM vehicles WHERE driver_id = ?',
                    (old_driver_id,)))

                if vehicle:
                    order_date = datetime.fromisoformat(
                        pickup_time.replace('Z', '+00:00')).date()
                    pickup_time_only = datetime.fromisoformat(
                        pickup_time.replace('Z', '+00:00')).time()
                    dropoff_time_only = datetime.fromisoformat(
                        dropoff_time.replace('Z', '+00:00')).time()

                    shift = db.fetchone(db.execute(conn, '''
                        SELECT * FROM shifts
                        WHERE driver_id = ?
                        AND shift_date = ?
                        AND start_time <= ?
                        AND end_time >= ?
                    ''', (old_driver_id, order_date.isoformat(),
                          pickup_time_only.strftime('%H:%M:%S'),
                          dropoff_time_only.strftime('%H:%M:%S'))))

                    if shift:
                        if weight <= vehicle['max_weight']:
                            overlapping = db.fetchone(db.execute(conn, '''
                                SELECT COUNT(*) as count
                                FROM orders
                                WHERE vehicle_id = ?
                                AND status IN ('assigned', 'completed')
                                AND DATE(pickup_time) = ?
                                AND id != ?
                                AND pickup_time < ? AND dropoff_time > ?
                            ''', (vehicle['id'], order_date.isoformat(), order_id,
                                  dropoff_time, pickup_time)))

                            if overlapping['count'] < vehicle['max_orders']:
                                driver_id = old_driver_id
                                vehicle_id = vehicle['id']

            if not driver_id:
                driver_id, vehicle_id = find_available_driver(
                    conn, pickup_time, dropoff_time, weight, exclude_driver_id=old_driver_id)

            if driver_id and vehicle_id:
                db.execute(conn, '''
                    UPDATE orders
                    SET driver_id = ?, vehicle_id = ?, status = 'assigned'
                    WHERE id = ?
                ''', (driver_id, vehicle_id, order_id))
            else:
                db.execute(conn, '''
                    UPDATE orders
                    SET driver_id = NULL, vehicle_id = NULL, status = 'pending'
                    WHERE id = ?
                ''', (order_id,))

        conn.commit()

        updated_order = db.fetchone(db.execute(conn, '''
            SELECT o.id as order_id, o.merchant_id, o.description, o.pickup_time,
                   o.dropoff_time, o.weight, o.status, d.id as driver_id, d.name as driver_name
            FROM orders o
            LEFT JOIN drivers d ON o.driver_id = d.id
            WHERE o.id = ?
        ''', (order_id,)))

        conn.close()

        response = {
            "order_id": updated_order['order_id'],
            "merchant_id": updated_order['merchant_id'],
            "description": updated_order['description'],
            "pickup_time": _serialize_timestamp(updated_order['pickup_time']),
            "dropoff_time": _serialize_timestamp(updated_order['dropoff_time']),
            "weight": updated_order['weight'],
            "status": updated_order['status']
        }

        if updated_order['driver_id']:
            response["driver"] = {
                "id": updated_order['driver_id'],
                "name": updated_order['driver_name']
            }

        return jsonify(response)


@app.route('/orders/<int:order_id>', methods=['DELETE'])
def delete_order(order_id):
    """Cancel an order and free up any driver/vehicle assignment immediately."""
    conn = get_db_connection()

    order = db.fetchone(db.execute(conn,
        'SELECT * FROM orders WHERE id = ?', (order_id,)))
    if not order:
        conn.close()
        return jsonify({"error": "Order not found"}), 404

    db.execute(conn, '''
        UPDATE orders
        SET status = 'cancelled', driver_id = NULL, vehicle_id = NULL
        WHERE id = ?
    ''', (order_id,))
    conn.commit()
    conn.close()

    return jsonify({"message": "Order cancelled and driver/vehicle assignment freed"}), 200


# ==================== AUTH ====================


@app.route('/auth/login', methods=['POST'])
def login():
    """Authenticate a merchant with email + password and return a JWT."""
    data = request.get_json() or {}
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    conn = get_db_connection()
    merchant = db.fetchone(db.execute(conn,
        'SELECT * FROM merchants WHERE email = ?', (email,)))
    conn.close()

    if not merchant or not merchant['password_hash']:
        return jsonify({"error": "Invalid credentials"}), 401

    if not check_password_hash(merchant['password_hash'], password):
        return jsonify({"error": "Invalid credentials"}), 401

    token = create_access_token(merchant)

    merchant_data = {
        "id": merchant["id"],
        "name": merchant["name"],
        "email": merchant["email"],
    }

    return jsonify({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": JWT_EXPIRATION_MINUTES * 60,
        "merchant": merchant_data,
    }), 200


@app.route('/upload', methods=['POST'])
def upload_csv():
    """Upload and process CSV file."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    csv_type = request.form.get('type')

    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    if not csv_type or csv_type not in ['merchants', 'drivers', 'vehicles', 'orders']:
        return jsonify({"error": "Invalid CSV type"}), 400

    if not file.filename.endswith('.csv'):
        return jsonify({"error": "File must be a CSV"}), 400

    try:
        import csv
        import io

        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_reader = csv.DictReader(stream)

        conn = get_db_connection()
        count = 0
        errors = []

        if csv_type == 'merchants':
            for row in csv_reader:
                try:
                    raw_password = row.get('password')
                    existing_hash = row.get('password_hash')

                    if raw_password:
                        password_hash = generate_password_hash(raw_password, method=PASSWORD_HASH_METHOD)
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
                except Exception as e:
                    errors.append(f"Row {count + 1}: {str(e)}")

        elif csv_type == 'drivers':
            for row in csv_reader:
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
                except Exception as e:
                    errors.append(f"Row {count + 1}: {str(e)}")

        elif csv_type == 'vehicles':
            for row in csv_reader:
                try:
                    if db.using_postgres():
                        db.execute(conn,
                            'INSERT INTO vehicles (id, driver_id, max_orders, max_weight) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING',
                            (row['id'], row['driver_id'],
                             row['max_orders'], row['max_weight']))
                    else:
                        db.execute(conn,
                            'INSERT OR IGNORE INTO vehicles (id, driver_id, max_orders, max_weight) VALUES (?, ?, ?, ?)',
                            (row['id'], row['driver_id'],
                             row['max_orders'], row['max_weight']))
                    count += 1
                except Exception as e:
                    errors.append(f"Row {count + 1}: {str(e)}")

        elif csv_type == 'orders':
            for row in csv_reader:
                try:
                    driver_id = row.get('driver_id') if row.get(
                        'driver_id') and row.get('driver_id').strip() else None
                    vehicle_id = row.get('vehicle_id') if row.get(
                        'vehicle_id') and row.get('vehicle_id').strip() else None
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
                            (row['id'], row['merchant_id'], driver_id, vehicle_id,
                             row.get('status', 'pending'), description,
                             row['pickup_time'], row['dropoff_time'], row['weight']))
                    else:
                        db.execute(conn,
                            'INSERT OR REPLACE INTO orders (id, merchant_id, driver_id, vehicle_id, status, description, pickup_time, dropoff_time, weight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            (row['id'], row['merchant_id'], driver_id, vehicle_id,
                             row.get('status', 'pending'), description,
                             row['pickup_time'], row['dropoff_time'], row['weight']))
                    count += 1
                except Exception as e:
                    errors.append(f"Row {count + 1}: {str(e)}")

        conn.commit()
        conn.close()

        if errors:
            return jsonify({
                "message": f"Uploaded {count} {csv_type} with {len(errors)} errors",
                "errors": errors[:10]
            }), 207

        return jsonify({
            "message": f"Successfully uploaded {count} {csv_type}",
            "count": count
        }), 200

    except Exception as e:
        return jsonify({"error": f"Failed to process CSV: {str(e)}"}), 500


@app.route('/')
def home():
    """Health check endpoint."""
    return jsonify({"message": "Order Management System is running!", "status": "healthy"})


@app.route('/admin/db-view', methods=['GET'])
def view_database():
    """View database contents for debugging."""
    table = request.args.get('table', 'orders')
    limit = request.args.get('limit', 100, type=int)

    if table not in ['merchants', 'drivers', 'vehicles', 'shifts', 'orders']:
        return jsonify({"error": "Invalid table name"}), 400

    conn = get_db_connection()

    if db.using_postgres():
        columns_cur = db.execute(conn,
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
            (table,))
        columns = [row['column_name'] for row in db.fetchall(columns_cur)]
    else:
        schema = conn.execute(f"PRAGMA table_info({table})").fetchall()
        columns = [col[1] for col in schema]

    data = db.fetchall(db.execute(conn, f"SELECT * FROM {table} LIMIT ?", (limit,)))

    total = db.fetchone(db.execute(conn,
        f"SELECT COUNT(*) as count FROM {table}"))['count']

    conn.close()

    return jsonify({
        "table": table,
        "columns": columns,
        "total_rows": total,
        "showing": len(data),
        "data": data
    })


@app.route('/merchants', methods=['GET'])
def get_merchants():
    """Get all merchants."""
    conn = get_db_connection()
    merchants = db.fetchall(db.execute(conn, 'SELECT * FROM merchants ORDER BY id'))
    conn.close()
    return jsonify(merchants)


@app.route('/merchants', methods=['POST'])
def create_merchant():
    """Create a new merchant."""
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')

    if not name or not email:
        return jsonify({"error": "Name and email are required"}), 400

    conn = get_db_connection()
    try:
        if db.using_postgres():
            cur = db.execute(conn,
                'INSERT INTO merchants (name, email) VALUES (?, ?) RETURNING id',
                (name, email))
            merchant_id = db.fetchone(cur)['id']
        else:
            cur = db.execute(conn,
                'INSERT INTO merchants (name, email) VALUES (?, ?)',
                (name, email))
            merchant_id = cur.lastrowid
        conn.commit()
        merchant = db.fetchone(db.execute(conn,
            'SELECT * FROM merchants WHERE id = ?', (merchant_id,)))
        conn.close()
        return jsonify(merchant), 201
    except _integrity_error():
        conn.rollback()
        conn.close()
        return jsonify({"error": "Merchant with this name or email already exists"}), 400


# ==================== WEBSOCKET TRACKING ====================

register_socketio_handlers(socketio)


if __name__ == '__main__':
    init_db()
    db_info = db.DATABASE_URL if db.using_postgres() else DATABASE_PATH
    print(f"Database initialized: {'PostgreSQL' if db.using_postgres() else 'SQLite'} ({db_info})")

    start_location_updates(socketio)
    print("Location tracking started (sending updates every 5 seconds)")

    port = int(os.getenv('PORT', '8000'))
    debug_mode = os.getenv('FLASK_DEBUG', 'true').lower() == 'true'
    allow_unsafe = os.getenv('ALLOW_UNSAFE_WERKZEUG', 'true').lower() == 'true'
    socketio.run(
        app,
        host='0.0.0.0',
        port=port,
        debug=debug_mode,
        allow_unsafe_werkzeug=allow_unsafe,
    )
