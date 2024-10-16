import sys
import threading
import requests
import datetime
import json
import time
import psycopg2
from psycopg2 import sql
from flask import Flask, jsonify
import urllib.parse

app = Flask(__name__)

# Load configuration from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Assign configuration variables
DB_CONFIG = config["database"]
TABLE_NAME = config["table_name"]
TIMEOUT_PERIOD = config["timeout_period"]
CASE_FILE = config["case_file"]
DELAY = config["delay"]
RETRY_DELAY = config["retry_delay"]
MAX_RETRIES = config["max_retries"]
BATCH_SIZE = config["batch_size"]
MAX_THREADS = config["max_threads"]
INITIAL_DELAY = config["initial_delay"]
PROXY = config["proxy"]

# colorful yeppp
INFO = "[\x1b[33m?\x1b[37m] "
ERROR = "[\x1b[31m-\x1b[37m] "
SUCCESS = "[\x1b[32m+\x1b[37m] "

@app.route('/getcases', methods=['GET'])
def get_cases():
    """Fetch all cases from the database and return as a JSON array."""
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query for the 'last_updated' timestamp
        cursor.execute(sql.SQL("SELECT MIN(last_updated) FROM {table}").format(table=sql.Identifier(TABLE_NAME)))
        oldest_update = cursor.fetchone()[0]

        # fetch all rows from the table
        query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(TABLE_NAME))
        cursor.execute(query)
        
        # Fetch all rows and column names
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

        # Convert the data to a list of dictionaries
        cases = [dict(zip(colnames, row)) for row in rows]
        
        # Close the connection
        cursor.close()
        conn.close()

        # Return data as JSON
        return jsonify({"last_updated": oldest_update, "cases": cases})

    except psycopg2.Error as error:
        return jsonify({"error": str(error)}), 500

def process_case(case, session):
    # URL-encode the market_hash_name parameter
    encoded_name = urllib.parse.quote(case['name'])
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=3&market_hash_name={encoded_name}"
    retries = 0

    while retries < MAX_RETRIES:
        try:
            # Attempt to retrieve data from the URL using the proxy
            response = session.get(url, timeout=TIMEOUT_PERIOD, proxies=PROXY)
            response.raise_for_status()  # Raise an error for bad codes
            data = response.json()

            # Extract data
            price = data.get('lowest_price', 'N/A')
            volume = data.get('volume', 'N/A')
            median_price = data.get('median_price', 'N/A')
            picture_url = case.get('image', 'N/A')
            last_updated = datetime.datetime.now()

            return (case['name'], price, volume, median_price, picture_url, last_updated)

        except requests.RequestException as e:
            retries += 1
            print(f"{ERROR}An error occurred for {case['name']} (attempt {retries}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY)

    print(f"{ERROR}Failed to retrieve data for {case['name']} after {MAX_RETRIES} attempts.")
    return None

def batch_insert_to_db(cases_batch, conn):
    if cases_batch:
        with conn.cursor() as cursor:
            cursor.executemany(sql.SQL(f"""
                INSERT INTO {TABLE_NAME} (name, price, volume, median_price, picture_url, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    median_price = EXCLUDED.median_price,
                    picture_url = EXCLUDED.picture_url,
                    last_updated = EXCLUDED.last_updated
            """), cases_batch)
        conn.commit()
        print(f"{SUCCESS}Batch of {len(cases_batch)} cases stored in database.")

def worker_thread(case_list, start_delay):
    time.sleep(start_delay)  # Stagger thread starts
    session = requests.Session()
    session.proxies.update(PROXY)
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    cases_batch = []
    while True:
        for case in case_list:
            processed_case = process_case(case, session)
            if processed_case:
                cases_batch.append(processed_case)

            # Insert the batch into the database
            if len(cases_batch) >= BATCH_SIZE:
                batch_insert_to_db(cases_batch, conn)
                cases_batch.clear()

        # Insert any remaining before next loop
        if cases_batch:
            batch_insert_to_db(cases_batch, conn)

        time.sleep(1)

    conn.close()

def main():
    try:
        # Load JSON data from the specified file
        with open(CASE_FILE, 'r') as f:
            case_list = json.load(f)
    except FileNotFoundError:
        print(f"{ERROR}Error: {CASE_FILE} not found.")
        return
    except json.JSONDecodeError:
        print(f"{ERROR}Error: Failed to parse JSON from {CASE_FILE}.")
        return

    # Check if file is empty
    if len(case_list) == 0:
        print(f"{ERROR}Chosen file is blank")
        return

    print(f"\n{INFO}Starting continuous processing for {len(case_list)} cases\n")

    # Start a Thread Pool with staggered start times
    threads = []
    for i in range(MAX_THREADS):
        t = threading.Thread(target=worker_thread, args=(case_list, i * INITIAL_DELAY))
        t.daemon = True
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    threading.Thread(target=main, daemon=True).start()
    app.run(debug=True)
