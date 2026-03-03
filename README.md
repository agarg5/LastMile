# Package Delivery Order Management System

An application to manage last-mile package delivery by coordinating drivers and orders.

A Python Flask backend with SQLite database and React+Tailwind frontend for managing orders, merchants, drivers, vehicles, and shifts.

See a video demo and explanation at https://www.loom.com/share/e2d512a7f6074f5eb1f9fe98502d3239

## Live Deployment

The app is deployed on Railway:

- **Frontend**: https://frontend-production-6952.up.railway.app
- **Backend API**: https://backend-production-9371.up.railway.app

Login with: `primeservices@example.com` / `prime-pass`

## Features
- authentication for merchants using JWT's. You can play around with the app by using these credentials (username: primeservices@example.com, password: prime-pass)
- allocation algorithm to match packages, drivers, shifts
- real-time map view showing where packages are
- ability to upload CSV files with more data
- responsive web design and pagination
- back-end and front-end tests
- data generation using Faker to seed the database

## Project Structure

```
.
├── backend/              # Backend Python application
│   ├── app.py            # Main Flask application
│   ├── requirements.txt  # Python dependencies
│   ├── generate_datasets.py  # Script to generate CSV datasets
│   ├── load_data.py      # Script to load CSV data into database
│   ├── test_app.py       # Test suite
│   ├── pytest.ini       # Pytest configuration
│   └── *.csv            # Generated CSV datasets
├── frontend/             # Frontend React application
│   ├── src/              # React source code
│   ├── package.json      # Node dependencies
│   └── vite.config.ts    # Vite configuration
├── data/                 # SQLite database storage (created automatically)
├── Dockerfile            # Docker configuration for backend
├── .dockerignore
└── README.md
```

## Database Schema

### Merchants
- `id`: Primary key
- `name`: Unique merchant name
- `email`: Unique email address
- `created_at`: Timestamp

### Drivers
- `id`: Primary key
- `name`: Unique driver name
- `created_at`: Timestamp

### Vehicles
- `id`: Primary key
- `driver_id`: Foreign key to drivers (one vehicle per driver)
- `max_orders`: Maximum number of orders (5-10)
- `max_weight`: Maximum weight capacity (100-500)
- `created_at`: Timestamp

### Shifts
- `id`: Primary key
- `driver_id`: Foreign key to drivers
- `shift_date`: Date of the shift
- `start_time`: Shift start time (e.g., 09:00:00)
- `end_time`: Shift end time (e.g., 17:00:00)
- `created_at`: Timestamp
- Unique constraint: one shift per driver per day

### Orders
- `id`: Primary key
- `merchant_id`: Foreign key to merchants
- `driver_id`: Foreign key to drivers (nullable, assigned when status changes to 'assigned')
- `vehicle_id`: Foreign key to vehicles (nullable, assigned when status changes to 'assigned')
- `status`: Order status - `pending`, `assigned`, `completed`, or `cancelled`
- `pickup_time`: Timestamp for pickup
- `dropoff_time`: Timestamp for dropoff
- `weight`: Order weight (10-200)
- `created_at`: Timestamp

## Order Statuses

- **pending**: Order created but no driver allocated yet
- **assigned**: A driver and vehicle have been allocated to the order
- **completed**: Order marked as complete by the driver
- **cancelled**: Order cancelled by merchant or driver

## Order Validation Rules

1. **Pickup time** must be at least **15 minutes** before dropoff time
2. **Dropoff time** must be at most **4 hours** after pickup time
3. Both pickup and dropoff must be on the **same day**

## Getting Started

### Build the Docker image:
```bash
docker build -t python-sqlite-backend .
```

### Run the container:
```bash
docker run -p 8000:8000 -v $(pwd)/data:/app/data python-sqlite-backend
```

The `-v $(pwd)/data:/app/data` flag mounts a local directory to persist the database.

### Generate Sample Data

1. Generate CSV datasets:
```bash
cd backend
python3 generate_datasets.py
```

This creates CSV files in the `backend/` directory:
- `merchants.csv` - 10 merchants
- `drivers.csv` - 50 drivers
- `shifts.csv` - 500 shifts (50 drivers × 10 days)
- `vehicles.csv` - 50 vehicles (one per driver)
- `orders.csv` - 1,000 orders

2. Load data into database:
```bash
cd backend
python3 load_data.py
```

## API Endpoints

### Health Check
- `GET /` - Health check endpoint

### Merchants
- `GET /merchants` - Get all merchants
- `POST /merchants` - Create a new merchant
  ```json
  {
    "name": "Acme Corp",
    "email": "contact@acme.com"
  }
  ```
- `GET /merchants/<id>` - Get merchant by ID

### Drivers
- `GET /drivers` - Get all drivers
- `POST /drivers` - Create a new driver
  ```json
  {
    "name": "John Doe"
  }
  ```
- `GET /drivers/<id>` - Get driver by ID

### Vehicles
- `GET /vehicles` - Get all vehicles (includes driver name)
- `POST /vehicles` - Create a new vehicle
  ```json
  {
    "driver_id": 1,
    "max_orders": 8,
    "max_weight": 300
  }
  ```
- `GET /vehicles/<id>` - Get vehicle by ID

### Shifts
- `GET /shifts` - Get all shifts (includes driver name)
- `POST /shifts` - Create a new shift
  ```json
  {
    "driver_id": 1,
    "shift_date": "2025-01-15",
    "start_time": "09:00:00",
    "end_time": "17:00:00"
  }
  ```
- `GET /shifts/<id>` - Get shift by ID

### Orders
- `GET /orders` - Get all orders (includes merchant, driver, vehicle info)
  - Query parameter: `?status=pending` - Filter by status
- `POST /orders` - Create a new order
  ```json
  {
    "merchant_id": 1,
    "pickup_time": "2025-01-15T10:00:00",
    "dropoff_time": "2025-01-15T12:00:00",
    "weight": 150.5
  }
  ```
- `GET /orders/<id>` - Get order by ID
- `PATCH /orders/<id>` - Update an order
  ```json
  {
    "status": "assigned",
    "driver_id": 1,
    "vehicle_id": 1
  }
  ```
- `DELETE /orders/<id>` - Delete an order

## Local Development (without Docker)

### Backend

1. Create a virtual environment:
```bash
cd backend
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
python app.py
```

### Frontend

1. Install dependencies:
```bash
cd frontend
npm install
```

2. Run the development server:
```bash
npm run dev
```

The frontend will be available at `http://localhost:3000` and will proxy API requests to the backend at `http://localhost:8000`.

## Testing the API

```bash
# Health check
curl http://localhost:8000/

# Create a merchant
curl -X POST http://localhost:8000/merchants \
  -H "Content-Type: application/json" \
  -d '{"name": "Acme Corp", "email": "contact@acme.com"}'

# Create a driver
curl -X POST http://localhost:8000/drivers \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe"}'

# Create a vehicle
curl -X POST http://localhost:8000/vehicles \
  -H "Content-Type: application/json" \
  -d '{"driver_id": 1, "max_orders": 8, "max_weight": 300}'

# Create a shift
curl -X POST http://localhost:8000/shifts \
  -H "Content-Type: application/json" \
  -d '{"driver_id": 1, "shift_date": "2025-01-15", "start_time": "09:00:00", "end_time": "17:00:00"}'

# Create an order
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "merchant_id": 1,
    "pickup_time": "2025-01-15T10:00:00",
    "dropoff_time": "2025-01-15T12:00:00",
    "weight": 150.5
  }'

# Get all orders
curl http://localhost:8000/orders

# Get pending orders
curl http://localhost:8000/orders?status=pending

# Assign driver to order
curl -X PATCH http://localhost:8000/orders/1 \
  -H "Content-Type: application/json" \
  -d '{"status": "assigned", "driver_id": 1, "vehicle_id": 1}'

# Update order status
curl -X PATCH http://localhost:8000/orders/1 \
  -H "Content-Type: application/json" \
  -d '{"status": "completed"}'
```

## Testing

### Install Test Dependencies

```bash
pip install -r requirements.txt
```

### Run Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=app --cov-report=html

# Run specific test file
pytest test_app.py

# Run specific test class
pytest test_app.py::TestOrderCreation

# Run specific test
pytest test_app.py::TestOrderCreation::test_create_order_with_valid_data
```

### Test Coverage

#### Backend Tests

The backend test suite includes:

- **Order Creation**: Valid data, missing fields, time validation, auto-assignment
- **Order Updates**: Merchant authorization, status checks, re-assignment logic
- **Order Cancellation**: Cancellation and driver/vehicle freeing
- **Order Retrieval**: Pagination, merchant filtering
- **Driver Endpoints**: Driver listing with shifts
- **Validation**: Time constraints, weight limits
- **Driver Assignment**: Shift availability, capacity checks

#### Frontend Tests

The frontend test suite includes:

- **CSV Upload**: File selection, type selection, upload success/error handling
- **Orders Page**: Rendering, order display, dialog interactions

### Running Frontend Tests

```bash
cd frontend
npm install  # Install test dependencies
npm test     # Run tests in watch mode
npm run test:ui  # Run tests with UI
npm run test:coverage  # Run tests with coverage report
```

### Manual Testing with CSV Files

Test CSV files are available in `frontend/test-data/`:

- `test_merchants.csv` - Sample merchants
- `test_drivers.csv` - Sample drivers
- `test_vehicles.csv` - Sample vehicles (requires drivers first)
- `test_orders.csv` - Sample orders (requires merchants first)

**To test manually:**
1. Navigate to the Upload CSV page
2. Select a data type
3. Upload the corresponding test CSV file
4. Verify the data appears in the respective pages

**Note:** Upload merchants and drivers before vehicles and orders due to foreign key constraints.
