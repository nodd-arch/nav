# HOW TO WORK WITH IT

0. Transfer Data to postgres

The data is now in /DEKUT_features.csv

create the database in postgres

change this in `pg.py` to your correct config "db_config"

make sure the path to `csv_file` also in "pg.py"

Run `python pg.py`.

Confirm in the database that the data is loaded

1. Install dependencies

run `pip install -r requirements.txt`

2. Start the Backend Server

run `python flask_server.py`

3. Open the HTML File

Simply open `main.html` in your web browser.

The page will automatically connect to the Flask API and load all features from your PostgreSQL database.