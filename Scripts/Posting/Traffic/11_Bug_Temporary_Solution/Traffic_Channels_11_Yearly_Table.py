import mysql.connector
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)
# Database connection details
db_config = {
    'host': os.getenv("DB_HOST"),
    'port': int(os.getenv("DB_PORT")),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'database': os.getenv("DB_NAME")
}
 # or your database host


tables_list = ['first', 'second', 'third', 'fourth']

def start_insert():
    try:

        for table in tables_list:
            # Establish a database connection
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            truncate_query = f"""truncate prod.final_{table};"""

            # Dynamic query to execute
            query1 = f"""
                CALL prod.traffic_channel_ytd_11_{table}();
            """
            
            cursor.execute(truncate_query)
            connection.commit()
            # Execute the query
            cursor.execute(query1)

            # Fetch the results
            result = []
            while True:
                # Fetch the result set
                rows = cursor.fetchall()
                if not rows:  # Break if no more rows
                    break
                result.extend(rows)

            # Print the results for verification
            for row in result:
                print(row)


            cursor.close()
            connection.close()

            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            # Prepare the insert query to store results
            insert_query = f"""
                INSERT INTO prod.final_{table} (label, hint, cat, series)
                VALUES (%s, %s, %s, %s);
            """

            # Loop through the results and insert into your_table
            for row in result:
                cursor.execute(insert_query, row)

            # Commit the transaction
            connection.commit()

            cursor.close()
            connection.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
