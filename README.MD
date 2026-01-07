# Dekut Offline Navigator

Offline-first navigation and feature viewer using PostgreSQL, Flask, and a static HTML frontend.

---

## Features

* CSV to PostgreSQL data ingestion
* Flask REST API
* Static HTML frontend
* Offline-capable local deployment
* Ready for PostGIS and Leaflet integration

---

## Tech Stack

* Backend: Python, Flask
* Database: PostgreSQL (PostGIS-ready)
* Frontend: HTML, JavaScript
* Data Source: CSV

---

## Requirements

* Python 3.9+
* PostgreSQL 14+
* pip
* PostGIS

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/nodd-arch/nav.git
cd nav
```

---

### 2. Load Data into PostgreSQL

#### 2.1 Prepare CSV

Ensure the file exists:

```
DEKUT_features.csv
```

---

#### 2.2 Create Database

```sql
CREATE DATABASE dekut_nav;
```

---

#### 2.3 Configure Database Connection

Edit `pg.py`:

```python
db_config = {
    "host": "localhost",
    "database": "dekut_nav",
    "user": "your_username",
    "password": "your_password",
    "port": 5432
}
```

Set the CSV path:

```python
csv_file = "your-own-path/DEKUT_features.csv"
```

---

#### 2.4 Import Data

```bash
python pg.py
```

Verify:

```sql
SELECT * FROM dekut_features;
```

---

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Start Backend Server

```bash
python flask_server.py
```

API runs at:

```
http://127.0.0.1:5000
```

---

### 5. Open Frontend

Open `http://localhost:5500/main.html` in your browser.

The frontend automatically fetches features from the Flask API.

---

## API Endpoints

### GET /features

Returns all features stored in the database.

---

## Project Structure

```
nav/
├── pg.py
├── flask_server.py
├── requirements.txt
├── DEKUT_features.csv
├── main.html
└── README.md
```

---
