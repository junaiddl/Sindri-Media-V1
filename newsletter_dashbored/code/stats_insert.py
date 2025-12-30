import os
import pandas as pd
import mysql.connector
from datetime import datetime

# -------- CONFIG --------

BASE_PATH = r"downloaded_files_v2"  # Root folder containing mailing list folders

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

TABLE_NAME = "total_subscriber_stats"

FOLDERS = ["subscribes", "unsubscribes", "whoissubscribed"]


# ---------------- DB CONNECTION ----------------

def connect_db():
    print("🔗 Connecting to MySQL database...")
    db = mysql.connector.connect(**DB_CONFIG)
    print("✅ Connection established")
    return db


# ---------------- INSERT FUNCTION ----------------

def insert_stats(cursor, stats_df):
    """Bulk insert stats into MySQL"""
    sql = f"""
        INSERT IGNORE INTO {TABLE_NAME} 
        (site_id, created_at, list_id, subscriber_count, subscribed_count, unsubscribed_count)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    values = list(stats_df[["site_id", "created_at", "list_id", 
                            "subscriber_count", "subscribed_count", "unsubscribed_count"]]
                  .itertuples(index=False, name=None))
    cursor.executemany(sql, values)
    return len(values)


# ---------------- MAIN PROCESS ----------------

def process_subscriber_stats():
    db = connect_db()
    cursor = db.cursor()
    all_stats = []

    for folder_name, meta in MAILING_METADATA.items():
        print(f"\n📂 Processing mailing list: {folder_name}")
        list_base = os.path.join(BASE_PATH, folder_name)

        # Dictionary to accumulate stats by date
        stats_dict = {}

        # ---------------- Whoissubscribed ----------------
        who_path = os.path.join(list_base, "whoissubscribed")
        if os.path.exists(who_path):
            who_files = [f for f in os.listdir(who_path) if f.endswith(".csv")]
            for file in who_files:
                file_path = os.path.join(who_path, file)
                print(f"➡️ Reading Whoissubscribed: {file}")
                df = pd.read_csv(file_path)

                if "subscriber_token" not in df.columns:
                    print(f"❌ Invalid file skipped: {file}")
                    continue

                # ----- Extract date from filename -----
                try:
                    file_date_str = file.split("_at_")[1].split("_")[0]
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                except Exception:
                    print(f"⚠️ Unable to parse date from filename: {file}")
                    continue

                # ----- Count unique subscriber tokens -----
                unique_count = df["subscriber_token"].nunique()

                # ----- Update stats -----
                stats_dict.setdefault(file_date, {"subscriber_count":0, "subscribed_count":0, "unsubscribed_count":0})
                stats_dict[file_date]["subscriber_count"] += unique_count

                print(f"📅 Using file date {file_date} → Unique Subscribers: {unique_count}")

        # ---------------- Subscribes ----------------
        sub_path = os.path.join(list_base, "subscribes")
        if os.path.exists(sub_path):
            sub_files = [f for f in os.listdir(sub_path) if f.endswith(".csv")]
            for file in sub_files:
                file_path = os.path.join(sub_path, file)
                print(f"➡️ Reading Subscribes: {file}")
                df = pd.read_csv(file_path)

                if "created_at" not in df.columns or "subscriber_token" not in df.columns:
                    print(f"❌ Invalid file skipped: {file}")
                    continue

                df["created_date"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
                df = df.dropna(subset=["created_date"])
                df["created_date"] = df["created_date"].dt.date  # Only date part

                daily_counts = df.groupby("created_date")["subscriber_token"].nunique()
                for date, count in daily_counts.items():
                    stats_dict.setdefault(date, {"subscriber_count":0, "subscribed_count":0, "unsubscribed_count":0})
                    stats_dict[date]["subscribed_count"] += count

        # ---------------- Unsubscribes ----------------
        unsub_path = os.path.join(list_base, "unsubscribes")
        if os.path.exists(unsub_path):
            unsub_files = [f for f in os.listdir(unsub_path) if f.endswith(".csv")]
            for file in unsub_files:
                file_path = os.path.join(unsub_path, file)
                print(f"➡️ Reading Unsubscribes: {file}")
                df = pd.read_csv(file_path)

                if "created_at" not in df.columns or "subscriber_token" not in df.columns:
                    print(f"❌ Invalid file skipped: {file}")
                    continue

                df["created_date"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
                df = df.dropna(subset=["created_date"])
                df["created_date"] = df["created_date"].dt.date  # Only date part

                daily_counts = df.groupby("created_date")["subscriber_token"].nunique()
                for date, count in daily_counts.items():
                    stats_dict.setdefault(date, {"subscriber_count":0, "subscribed_count":0, "unsubscribed_count":0})
                    stats_dict[date]["unsubscribed_count"] += count

        # ---------------- Prepare DataFrame ----------------
        stats_list = []
        for date, counts in stats_dict.items():
            stats_list.append({
                "site_id": SITE_ID,
                "created_at": date,
                "list_id": meta["list_id"],
                "subscriber_count": counts["subscriber_count"],
                "subscribed_count": counts["subscribed_count"],
                "unsubscribed_count": counts["unsubscribed_count"]
            })
        stats_df = pd.DataFrame(stats_list)

        # Bulk insert into DB
        if not stats_df.empty:
            inserted_rows = insert_stats(cursor, stats_df)
            db.commit()
            print(f"✔ Stats inserted for mailing list {folder_name} → {inserted_rows} rows")
            all_stats.append(stats_df)

    # ---------------- PRINT ALL INSERTED STATS ----------------
    print("\n📊 Final Inserted Subscriber Stats:")
    if all_stats:
        final_df = pd.concat(all_stats).sort_values(by=["list_id", "created_at"])
        print(final_df.to_string(index=False))
    else:
        print("⚠️ No stats were inserted.")

    cursor.close()
    db.close()
    print("🔒 Database connection closed")
    print("✅ All subscriber stats processed and inserted.")


# ---------------- RUN ----------------

def execution_stats():
    print("🚀 Starting Subscriber Stats processing...")
    process_subscriber_stats()


if __name__ == "__main__":
    execution_stats()