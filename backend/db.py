import os

# Database configuration
# If DATABASE_URL is set (Railway PostgreSQL), use PostgreSQL.
# Otherwise fall back to SQLite for local development.
DATABASE_URL = os.getenv('DATABASE_URL')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_PATH = os.getenv('DATABASE_PATH', os.path.join(
    SCRIPT_DIR, '..', 'data', 'database.db'))


def using_postgres():
    """Return True if we're configured to use PostgreSQL."""
    return bool(DATABASE_URL)


def get_db_connection():
    """Create a database connection (PostgreSQL or SQLite)."""
    if using_postgres():
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        return conn
    else:
        import sqlite3
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        return conn


def execute(conn, sql, params=None):
    """Execute SQL with automatic placeholder conversion.

    Write SQL using ? placeholders (SQLite style).
    When using PostgreSQL, they are converted to %s automatically.
    Returns a cursor.
    """
    if using_postgres():
        import psycopg2.extras
        sql = sql.replace('?', '%s')
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = conn.cursor()

    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    return cur


def fetchone(cursor):
    """Fetch one row as a dict."""
    row = cursor.fetchone()
    if row is None:
        return None
    if using_postgres():
        return dict(row)
    return dict(row)


def fetchall(cursor):
    """Fetch all rows as dicts."""
    rows = cursor.fetchall()
    if using_postgres():
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]


def lastrowid(cursor, conn, table):
    """Get the last inserted row ID (handles both backends)."""
    if using_postgres():
        # For PostgreSQL, we use RETURNING id in the INSERT statement
        # This function is a fallback using currval
        cur = conn.cursor()
        cur.execute(f"SELECT currval(pg_get_serial_sequence('{table}', 'id'))")
        return cur.fetchone()[0]
    else:
        return cursor.lastrowid


def init_db():
    """Initialize the database with all required tables."""
    conn = get_db_connection()

    if using_postgres():
        _init_postgres(conn)
    else:
        _init_sqlite(conn)

    conn.commit()
    conn.close()


def _init_postgres(conn):
    """Initialize PostgreSQL schema."""
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS merchants (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS vehicles (
            id SERIAL PRIMARY KEY,
            driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
            max_orders INTEGER NOT NULL,
            max_weight REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(driver_id)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS shifts (
            id SERIAL PRIMARY KEY,
            driver_id INTEGER NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
            shift_date DATE NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(driver_id, shift_date)
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            merchant_id INTEGER NOT NULL REFERENCES merchants(id) ON DELETE CASCADE,
            driver_id INTEGER REFERENCES drivers(id) ON DELETE SET NULL,
            vehicle_id INTEGER REFERENCES vehicles(id) ON DELETE SET NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            description TEXT,
            pickup_time TIMESTAMP NOT NULL,
            dropoff_time TIMESTAMP NOT NULL,
            weight REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK(status IN ('pending', 'assigned', 'completed', 'cancelled'))
        )
    ''')

    cur.close()


def _init_sqlite(conn):
    """Initialize SQLite schema."""
    import sqlite3

    conn.execute('''
        CREATE TABLE IF NOT EXISTS merchants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS drivers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS vehicles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_id INTEGER NOT NULL,
            max_orders INTEGER NOT NULL,
            max_weight REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE CASCADE,
            UNIQUE(driver_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_id INTEGER NOT NULL,
            shift_date DATE NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE CASCADE,
            UNIQUE(driver_id, shift_date)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            merchant_id INTEGER NOT NULL,
            driver_id INTEGER,
            vehicle_id INTEGER,
            status TEXT NOT NULL DEFAULT 'pending',
            description TEXT,
            pickup_time TIMESTAMP NOT NULL,
            dropoff_time TIMESTAMP NOT NULL,
            weight REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (merchant_id) REFERENCES merchants(id) ON DELETE CASCADE,
            FOREIGN KEY (driver_id) REFERENCES drivers(id) ON DELETE SET NULL,
            FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL,
            CHECK(status IN ('pending', 'assigned', 'completed', 'cancelled'))
        )
    ''')

    try:
        conn.execute('ALTER TABLE orders ADD COLUMN description TEXT')
    except sqlite3.OperationalError:
        pass

    try:
        conn.execute('ALTER TABLE merchants ADD COLUMN password_hash TEXT')
    except sqlite3.OperationalError:
        pass
