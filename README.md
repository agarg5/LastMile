# LastMile - Package Delivery Order Management System

A full-stack application for managing last-mile package delivery, coordinating merchants, drivers, vehicles, and orders.

**Live app**: https://frontend-production-6952.up.railway.app (credentials are pre-filled, just click Sign in)

**Video demo**: https://www.loom.com/share/e2d512a7f6074f5eb1f9fe98502d3239

## Features

- **Order management** - Create, assign, track, and cancel delivery orders with time and weight validation
- **Driver allocation algorithm** - Automatically matches orders to available drivers based on shift schedules, vehicle capacity, and weight limits
- **Real-time tracking** - Live map view showing package locations using WebSockets and Leaflet
- **CSV bulk import** - Upload merchants, drivers, vehicles, and orders via CSV
- **JWT authentication** - Merchant-scoped login with protected routes
- **Responsive UI** - Works on desktop and mobile

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React, TypeScript, Tailwind CSS, Vite |
| Backend | Python, Flask, Flask-SocketIO |
| Database | PostgreSQL (production), SQLite (local dev) |
| Deployment | Railway (Docker containers) |
| Maps | Leaflet + OpenStreetMap |
| Auth | JWT (PyJWT) |
| Testing | Pytest (backend), Vitest (frontend) |

## Architecture

```
frontend/          React SPA served via nginx
  src/
    pages/         LoginPage, OrdersPage, DriversPage, TrackingPage, UploadCSV
    api/client.ts  API client with JWT auth headers
    components/    Reusable UI components (shadcn/ui)

backend/
  app.py           Flask routes, JWT auth, order CRUD, driver assignment
  db.py            Database abstraction (PostgreSQL + SQLite dual-mode)
  orders_service.py  Order assignment algorithm
  websocket_service.py  Real-time tracking via Socket.IO
  load_data.py     Seed database with generated data
```

## Running Locally

```bash
# Backend
cd backend
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python3 generate_datasets.py && python3 load_data.py
python app.py  # http://localhost:8000

# Frontend
cd frontend
npm install
npm run dev  # http://localhost:3000
```

## Tests

```bash
# Backend
cd backend && pytest -v

# Frontend
cd frontend && npm test
```
