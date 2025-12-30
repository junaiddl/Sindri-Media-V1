import os
import re
import pandas as pd
import mysql.connector
import math

from datetime import datetime, timedelta


# -------- CONFIG --------

BASE_PATH = r"downloaded_files_v2"  # Root folder where files exist

MAILING_METADATA = {
    "morgen": {"list_id": 1, "mailing_name": "morgen"},
    "eftermiddag": {"list_id": 2, "mailing_name": "eftermiddag"},
    "guide": {"list_id": 3, "mailing_name": "guide"}
}

SITE_ID = 14

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT")),
    "database": "pre_stage"
}

TABLE_NAME = "whoissubscribed_data"
BATCH_SIZE = 10000

def extract_date_from_filename(filename):
    """Extract YYYY-MM-DD from filename."""
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    return match.group(0) if match else None
# ---------------- DB CONNECTION ----------------

def connect_db():
    print("🔗 Connecting to MySQL database...")
    db = mysql.connector.connect(**DB_CONFIG)
    print("✅ Connection established")
    return db


# ---------------- SANITIZER ----------------

def clean_value(value):
    """Convert pandas NaN or empty string into None."""
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None  # handles numpy.nan and pandas.NaT

    value = str(value).strip()

    if value == "" or value.lower() in ["nan", "none"]:
        return None

    return value


# ---------------- INSERT FUNCTION (with batching) ----------------

def insert_csv(cursor, df, meta, extracted_date):

    sql = f"""
        INSERT IGNORE INTO {TABLE_NAME}
        (site_id, list_id, mailing_name, created_at, subscriber_token, subscriber_group, subscriber_sindri,
         subscriber_birthdate, subscriber_gender, subscriber_zip, subscriber_position_of_trust)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = []

    for _, row in df.iterrows():

        
      
        created_date = extracted_date
     

        # birthdate conversion
        birth_raw = clean_value(row.get("subscriber_birthdate"))
        birthdate = (
            pd.to_datetime(birth_raw, errors="coerce", dayfirst=True).date()
            if birth_raw else None
        )

        values.append((
            SITE_ID,
            meta["list_id"],
            meta["mailing_name"],
            created_date,
            clean_value(row.get("subscriber_token")),
            clean_value(row.get("subscriber_group")),
            clean_value(row.get("subscriber_sindri")),
            birthdate,
            clean_value(row.get("subscriber_gender")),
            clean_value(row.get("subscriber_zip")),
            clean_value(row.get("subscriber_position_of_trust"))
        ))

    inserted_count = 0

    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i:i + BATCH_SIZE]
        cursor.executemany(sql, batch)
        inserted_count += len(batch)
        print(f"📦 Inserted batch of {len(batch)} rows (total so far: {inserted_count})")

    return inserted_count



# ---------------- MAIN PROCESS ----------------

def process_whoissubscribed():
    db = connect_db()
    cursor = db.cursor()
    total_inserted = 0

    for folder_name, meta in MAILING_METADATA.items():

        print(f"\n📂 Processing mailing list: {folder_name}")

        who_path = os.path.join(BASE_PATH, folder_name, "whoissubscribed")
        print(f"Looking for files in: {who_path}")
        exit

        if not os.path.exists(who_path):
            print(f"⚠️ Missing folder: {who_path}")
            continue

        files = [f for f in os.listdir(who_path) if f.endswith(".csv")]

        for file in files:
            date_str  = extract_date_from_filename(file)
            extracted_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            extracted_date = extracted_date - timedelta(days=1)

            print(f"➡️ Reading file: {file}")
            df = pd.read_csv(os.path.join(who_path, file))

            if "created_at" not in df.columns or "subscriber_token" not in df.columns:
                print(f"❌ Skipping invalid file: {file}")
                continue

            inserted = insert_csv(cursor, df, meta, extracted_date)
            db.commit()

            print(f"✔ Inserted {inserted} rows from {file}")
            total_inserted += inserted

    cursor.close()
    db.close()

    print("\n🎉 DONE!")
    print(f"📊 Total rows inserted: {total_inserted}")



# ---------------- RUN ----------------

def execution_whoissubscribed():
    print("🚀 Starting Whoissubscribed Data Load...")
    process_whoissubscribed()
 

if __name__ == "__main__":
    execution_whoissubscribed()