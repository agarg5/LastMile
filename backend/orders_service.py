from datetime import datetime, timedelta

import db


def validate_order_times(pickup_time_str, dropoff_time_str):
    """Validate order pickup and dropoff times."""
    try:
        pickup_time = datetime.fromisoformat(
            pickup_time_str.replace('Z', '+00:00'))
        dropoff_time = datetime.fromisoformat(
            dropoff_time_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return False, "Invalid datetime format. Use ISO format (YYYY-MM-DDTHH:MM:SS)"

    if pickup_time.date() != dropoff_time.date():
        return False, "Pickup and dropoff must be on the same day"

    time_diff = dropoff_time - pickup_time
    if time_diff < timedelta(minutes=15):
        return False, "Pickup time must be at least 15 minutes before dropoff time"

    if time_diff > timedelta(hours=4):
        return False, "Dropoff time must be at most 4 hours after pickup time"

    return True, None


def find_available_driver(conn, pickup_time_str, dropoff_time_str, weight, exclude_driver_id=None):
    """Find an available driver + vehicle for a new or updated order.

    Assignment rules / assumptions:
    - A driver is eligible only if they have a shift whose (date, start_time, end_time)
      fully covers the order's pickup and dropoff time window.
    - Each driver has exactly one vehicle; we enforce the vehicle's:
      * max_weight: the order's weight must be <= max_weight
      * max_orders: maximum number of *overlapping* orders that vehicle can carry.
    - The algorithm is greedy: it returns the first driver that satisfies all constraints.
    - When exclude_driver_id is provided, that driver is skipped entirely.
    """
    try:
        pickup_time = datetime.fromisoformat(
            pickup_time_str.replace('Z', '+00:00'))
        dropoff_time = datetime.fromisoformat(
            dropoff_time_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None, None

    order_date = pickup_time.date()
    pickup_time_only = pickup_time.time()
    dropoff_time_only = dropoff_time.time()

    shifts = db.fetchall(db.execute(conn, '''
        SELECT s.*, d.id as driver_id, d.name as driver_name, v.id as vehicle_id,
               v.max_orders, v.max_weight
        FROM shifts s
        JOIN drivers d ON s.driver_id = d.id
        JOIN vehicles v ON v.driver_id = d.id
        WHERE s.shift_date = ?
        AND s.start_time <= ?
        AND s.end_time >= ?
    ''', (order_date.isoformat(), pickup_time_only.strftime('%H:%M:%S'),
          dropoff_time_only.strftime('%H:%M:%S'))))

    for shift in shifts:
        driver_id = shift['driver_id']
        vehicle_id = shift['vehicle_id']
        max_orders = shift['max_orders']
        max_weight = shift['max_weight']

        if exclude_driver_id and driver_id == exclude_driver_id:
            continue

        if weight > max_weight:
            continue

        overlapping_orders = db.fetchone(db.execute(conn, '''
            SELECT COUNT(*) as count
            FROM orders
            WHERE vehicle_id = ?
            AND status IN ('assigned', 'completed')
            AND DATE(pickup_time) = ?
            AND pickup_time < ? AND dropoff_time > ?
        ''', (vehicle_id, order_date.isoformat(),
              dropoff_time_str, pickup_time_str)))

        current_order_count = overlapping_orders['count'] if overlapping_orders else 0

        if current_order_count >= max_orders:
            continue

        return driver_id, vehicle_id

    return None, None


def assign_driver_to_order(conn, order_id, pickup_time, dropoff_time, weight, exclude_driver_id=None):
    """Assign a driver to an order if possible."""
    driver_id, vehicle_id = find_available_driver(
        conn, pickup_time, dropoff_time, weight, exclude_driver_id)

    if driver_id and vehicle_id:
        db.execute(conn, '''
            UPDATE orders
            SET driver_id = ?, vehicle_id = ?, status = 'assigned'
            WHERE id = ?
        ''', (driver_id, vehicle_id, order_id))
        conn.commit()
        return driver_id, vehicle_id
    else:
        db.execute(conn, '''
            UPDATE orders
            SET driver_id = NULL, vehicle_id = NULL, status = 'pending'
            WHERE id = ?
        ''', (order_id,))
        conn.commit()
        return None, None
