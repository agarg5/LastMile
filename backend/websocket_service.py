import random
import threading
import time
from datetime import datetime

from flask_socketio import SocketIO, emit

import db


def generate_fake_location(socketio: SocketIO):
    """Generate fake driver locations for tracking and emit via SocketIO."""
    conn = db.get_db_connection()
    drivers = db.fetchall(db.execute(conn, 'SELECT id, name FROM drivers'))
    conn.close()

    if not drivers:
        return

    for driver in drivers:
        # Random location around NYC area
        latitude = 40.7128 + random.uniform(-0.1, 0.1)
        longitude = -74.0060 + random.uniform(-0.1, 0.1)

        location_update = {
            'driver_id': driver['id'],
            'driver_name': driver['name'],
            'latitude': round(latitude, 6),
            'longitude': round(longitude, 6),
            'timestamp': datetime.now().isoformat()
        }

        socketio.emit('location_update', location_update)


def register_socketio_handlers(socketio: SocketIO):
    """Register SocketIO connect/disconnect handlers on the given instance."""

    @socketio.on('connect')
    def handle_connect():  # pragma: no cover - simple logging
        print('Client connected to WebSocket')
        emit('connected', {'message': 'Connected to tracking server'})

    @socketio.on('disconnect')
    def handle_disconnect():  # pragma: no cover - simple logging
        print('Client disconnected from WebSocket')


def start_location_updates(socketio: SocketIO):
    """Start a background thread sending fake location updates every few seconds."""

    def send_updates():
        while True:
            generate_fake_location(socketio)
            time.sleep(5)  # Send updates every 5 seconds

    thread = threading.Thread(target=send_updates, daemon=True)
    thread.start()
    return thread
