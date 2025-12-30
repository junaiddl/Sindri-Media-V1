import os
import re
import pandas as pd
import mysql.connector


# -------- CONFIG --------

BASE_PATH = r"downloaded_files_v2"  # Root folder where files exist

MAILING_METADATA = {
    "morgen": {"list_id": 1},
    "eftermiddag": {"list_id": 2},
    "guide": {"list_id": 3}
}

SITE_ID = 14  # optional keep if needed

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT")),
    "database": "pre_stage"
}

TABLE_NAME = "whoissubscribed_unique_tokens"
BATCH_SIZE = 1000


# -------- HELPERS --------

def extract_date_from_filename(filename):
    """Extract YYYY-MM-DD from filename."""
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    return match.group(0) if match else None


def clean_token(value):
    """Ensure token is valid or return None."""
    if pd.isna(value):
        return None
    val = str(value).strip()
    return val if val else None


def connect_db():
    print("🔗 Connecting to MySQL...")
    db = mysql.connector.connect(**DB_CONFIG)
    print("✅ Connected")
    return db


# -------- INSERT LOGIC --------

def insert_records(cursor, records):

    sql = f"""
        INSERT IGNORE INTO {TABLE_NAME}
        (list_id, subscriber_token, created_at)
        VALUES (%s, %s, %s)
    """

    count = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        cursor.executemany(sql, batch)
        count += len(batch)
        print(f"📦 Inserted {len(batch)} (total so far: {count})")

    return count


# -------- MAIN LOGIC --------

def process_files():

    db = connect_db()
    cursor = db.cursor()
    total_inserted = 0

    for folder, meta in MAILING_METADATA.items():

        print(f"\n📂 Processing: {folder}")

        who_path = os.path.join(BASE_PATH, folder, "whoissubscribed")

        if not os.path.exists(who_path):
            print(f"⚠️ Missing folder: {who_path}")
            continue

        files = [f for f in os.listdir(who_path) if f.endswith(".csv")]

        for file in files:

            extracted_date = extract_date_from_filename(file)

            if not extracted_date:
                print(f"❌ No date found in filename, skipping: {file}")
                continue

            print(f"➡️ File: {file} | 📅 Parsed Date: {extracted_date}")

            df = pd.read_csv(os.path.join(who_path, file))

            if "subscriber_token" not in df.columns:
                print(f"❌ Missing token column, skipping: {file}")
                continue

            records = []

            for _, row in df.iterrows():
                token = clean_token(row.get("subscriber_token"))
                if token:
                    records.append((meta["list_id"], token, extracted_date))

            inserted = insert_records(cursor, records)
            db.commit()

            print(f"✔ Inserted {inserted} from {file}")
            total_inserted += inserted

    cursor.close()
    db.close()

    print("\n🎉 Completed!")
    print(f"📊 Total inserted: {total_inserted}")


# -------- RUN --------


def cleanup_data():
    db = connect_db()
    cursor = db.cursor()
    truncate_query = f"TRUNCATE TABLE {TABLE_NAME}"
    cursor.execute(truncate_query)
    db.commit()
    cursor.close()
    db.close()


def insert_masterlist_records():
    db = connect_db()
    cursor = db.cursor()
    # Insert logic here
    insert_query = f"""INSERT INTO  pre_stage.subscriber_master_list(
                        date,
                        subscriber_count,
                        subscribed_count,
                        unsubscribed_count,
                        site_id
                    )
                    WITH sub_unsub AS (
                        SELECT 
                            DATE(created_at) AS date,
                            SUM(subscribed_count) AS subscribed,
                            SUM(unsubscribed_count) AS unsubscribed
                        FROM pre_stage.total_subscriber_stats
                        GROUP BY DATE(created_at)
                    ),
                    whosubscribe AS (
                        SELECT 
                            DATE(created_at) AS date,
                            COUNT(DISTINCT subscriber_token) AS subscriber
                        FROM pre_stage.whoissubscribed_unique_tokens
                        GROUP BY DATE(created_at)
                    )

                    SELECT 
                        s.date,
                        w.subscriber as subscriber_count,
                        s.subscribed as subscribed_count,
                        s.unsubscribed as unsubscribed_count,
                        14 as site_id
                    FROM sub_unsub s
                    JOIN whosubscribe w ON s.date = w.date
                    ORDER BY s.date;"""
    cursor.execute(insert_query)
    db.commit()
    cursor.close()
    db.close()




def masterlist():
    print("🚀 Starting Unique Token Load...")
    cleanup_data()
    process_files()
    insert_masterlist_records()


if __name__ == "__main__":
    masterlist()
