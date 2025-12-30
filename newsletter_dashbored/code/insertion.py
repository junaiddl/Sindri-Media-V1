import os
import pandas as pd
import mysql.connector
from datetime import datetime

# -------- CONFIG --------

BASE_PATH = r"downloaded_files_v2"  # root folder where mailing list folders exist

MAILING_METADATA = {
    "morgen": {"mailing_list": 1, "siteid": 14, "mailing_name": "morgen"},
    "eftermiddag": {"mailing_list": 2, "siteid": 14, "mailing_name": "eftermiddag"},
    "guide": {"mailing_list": 3, "siteid": 14, "mailing_name": "guide"}
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT")),
    "database": "pre_stage"
}

TABLE_NAME = "click_events"


# ---------------- DB CONNECTION ----------------

def connect_db():
    print("🔗 Connecting to MySQL database...")
    db = mysql.connector.connect(**DB_CONFIG)
    print("✅ Connection established")
    return db


# ---------------- INSERT FUNCTION ----------------

def insert_csv(cursor, df):
    """Insert the whole DataFrame at once using executemany"""
    sql = f"""
        INSERT IGNORE INTO {TABLE_NAME}
        (created_date, newsletter_id, ref_url, subscriber_token, siteid, mailing_list, mailing_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples and replace NaN with None
    df = df.where(pd.notnull(df), None)
    values = list(df[["created_date", "newsletter_id", "ref_url", "subscriber_token",
                      "siteid", "mailing_list", "mailing_name"]].itertuples(index=False, name=None))
    
    cursor.executemany(sql, values)


    return len(values)

    


# ---------------- MAIN PROCESS ----------------

def process_clicks():
    db = connect_db()
    cursor = db.cursor()

    summary = {}

    for folder, meta in MAILING_METADATA.items():
        click_folder = os.path.join(BASE_PATH, folder, "clicks")
        print(f"\n📂 Processing folder: {folder}")

        if not os.path.exists(click_folder):
            print(f"⚠️ Folder missing: {click_folder}, skipping...")
            continue

        files = [f for f in os.listdir(click_folder) if f.endswith(".csv")]
        if not files:
            print(f"⚠️ No CSV files found in {click_folder}")
            continue

        print(f"📄 {len(files)} CSV files found, starting processing...")

        folder_insert_count = 0

        for file in files:
            file_path = os.path.join(click_folder, file)
            print(f"\n➡️ Reading file: {file}")

            try:
                df = pd.read_csv(file_path)
            except Exception as e:
                print(f"❌ Failed to read {file}: {e}")
                continue

            required_columns = ["created_at", "newsletter_id", "ref_url", "subscriber_token"]
            if not all(col in df.columns for col in required_columns):
                print(f"❌ Skipped (invalid columns): {file}")
                continue

            # Convert datetime → date only
            df["created_date"] = pd.to_datetime(df["created_at"], errors="coerce").dt.date

            # Add metadata
            df["siteid"] = meta["siteid"]
            df["mailing_list"] = meta["mailing_list"]
            df["mailing_name"] = meta["mailing_name"]

            # Insert all rows at once
            inserted_rows = insert_csv(cursor, df)
            db.commit()
            folder_insert_count += inserted_rows

            print(f"✔ File processed → {file} → {inserted_rows} rows inserted")

        summary[f"{folder}/Clicks"] = folder_insert_count
        print(f"📊 Finished folder: {folder} → {folder_insert_count} total rows inserted")

    remove_slash = f"""
        UPDATE pre_stage.click_events
        SET ref_url = TRIM(
                        TRAILING '/'
                        FROM SUBSTRING_INDEX(ref_url, '?', 1)
                    )
        WHERE ref_url IS NOT NULL;
"""
    cursor.execute(remove_slash)
    db.commit()
    print("🔒 Closing database connection...")

    cursor.close()
    db.close()
    print("🔒 Database connection closed")

    return summary


# ---------------- RUN SCRIPT ----------------

def execution_clicks():
    print("🚀 Starting Click CSV processing script...")
    results = process_clicks()

    print("\n================= INSERT SUMMARY =================")
    for folder, count in results.items():
        print(f"📌 {folder} → {count} records inserted")
    print("==================================================")
    print("✅ Script completed successfully!")


if __name__ == "__main__":
    execution_clicks()