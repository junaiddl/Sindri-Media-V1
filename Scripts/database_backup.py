import pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Database connection details
prod_db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT")),
}

test_db_config = {
    "host": os.getenv("DB_HOST_BKUP"),
    "user": os.getenv("DB_USER_BKUP"),
    "password": os.getenv("DB_PASSWORD_BKUP"),
    "port": int(os.getenv("DB_PORT_BKUP")),
}

# Tables to copy
tables = ["prod.traffic_channels", "prod.pages", "prod.events", "prod.daily_totals"]

# Number of days to look back
DAYS_BACK = 39

def delete_existing_data(src_cursor, table, siteid_column, postid_column, dest_cursor, date_column, siteid):
    """Delete rows from the source table if they exist in the destination table."""
    print(f"Checking and deleting matching data in {table}...")

    # Calculate the date for the last 7 days
    last_week_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')

    # Fetch matching siteid and postid for the last week's data from the source table
    src_cursor.execute(
        f"SELECT {siteid_column}, {postid_column} FROM {table} WHERE {date_column} >= %s AND siteid = %s",
        (last_week_date, siteid,)
    )
    dest_rows = src_cursor.fetchall()

    print(last_week_date)
    print(dest_rows)
    print(len(dest_rows))
    print(f"Fetched {len(dest_rows)} rows from {table} for the last week's data.")
    
    if dest_rows:
        # Build a condition for the delete query
        conditions = " OR ".join(
            f"({siteid_column} = %s AND {postid_column} = %s)" for _ in dest_rows
        )
        query = f"DELETE FROM {table} WHERE {conditions}"

        # Flatten the list of tuples into a single list for parameter substitution
        query_params = [value for row in dest_rows for value in row]

        # Execute delete query
        rows_deleted = dest_cursor.execute(query, query_params)
        print(f"Deleted {rows_deleted} rows from {table} in the source database.")
    else:
        print(f"No matching data found in the destination {table} for the last week's data.")


def copy_table_data(src_cursor, dest_cursor, table, date_column, siteid):
    """
    Copies data from the source table to the destination table for the last 7 days.
    Inserts rows one by one to avoid overload and connection issues.
    """
    last_week_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')

    # Fetch data from the source table for the last 7 days
    query = f"SELECT * FROM {table} WHERE {date_column} >= %s AND siteid = %s"
    src_cursor.execute(query, (last_week_date, siteid))
    rows = src_cursor.fetchall()

    print("Rows Fetched From Source Table")
    print(len(rows))

    if rows:
        # Get column names
        columns = [desc[0] for desc in src_cursor.description]
        column_names = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        # Insert data into the destination table row by row
        insert_query = f"""
        INSERT INTO {table} ({column_names}) VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {", ".join([f"{col}=VALUES({col})" for col in columns])};
        """
        
        for row in rows:
            try:
                dest_cursor.execute(insert_query, row)
                print(f"Inserted row: {row}")
            except pymysql.MySQLError as e:
                print(f"Error inserting row {row}: {e}")
    else:
        print(f"No data to copy for {table} in the last {DAYS_BACK} days.")


def main():
    sites = [3,4,10,11,13,14,15]
    id = 16
    for id in sites:
        try:
            # Connect to both databases
            prod_conn = pymysql.connect(**prod_db_config, connect_timeout=10000, charset='utf8mb4')
            test_conn = pymysql.connect(**test_db_config, connect_timeout=10000, charset='utf8mb4')

            # delete_existing_data(prod_conn.cursor(), "prod.site_archive_post", "siteid", "id", test_conn.cursor(), "date")
            
            with prod_conn.cursor() as prod_cursor, test_conn.cursor() as test_cursor:
                for table in tables:
                    print(f"Copying data for table: {table}")

                    # Specify the name of the date column for filtering
                    date_column = "date"  # Update this if your column name is different
                    
                    # Copy data for the last 7 days
                    copy_table_data(prod_cursor, test_cursor, table, date_column, id)
                
                # Commit changes to the testing database
                test_conn.commit()
                print("Data copy operation completed.")

            prod_conn.close()
            test_conn.close()
        
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
