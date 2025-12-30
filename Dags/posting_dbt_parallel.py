import mysql.connector
from procedure_creation_1 import main, create_procedure_in_db
from updation_proc_name import update_proc_name, drop_procedure
from testing_dbt import dev_etl
from datetime import datetime, timedelta

# Establish a connection to the MySQL database
def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host='51.158.56.32',
            user='Site',
            password='515B]_nP0;<|=pJOh35I',
            database='prod',
            port='1564',
        )
        cursor = connection.cursor()
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None
    
def truncate_data_quality(site):
    cursor, connection = get_database_connection()

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    print(current_date)

    sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND traffic_status IS NULL AND siteid = {site}"""
    
    cursor.execute(sql_query)
    connection.commit()  # Commit the transaction to save the changes
    print(cursor.rowcount, "record(s) deleted.")

    cursor.close()
    connection.close()

def execute(id):
    truncate_data_quality(id)
    try:
        # Try executing all the functions
        # drop_procedure(id)
        # create_procedure_in_db(id)
        # update_proc_name(id)
        dev_etl(id)
        send_data_quality(id, True)
    except Exception as e:
        # If any exception occurs, handle it here
        print(f"An error occurred while processing ID {id}: {e}")
            
        # Insert error log in the SQL table
        send_data_quality(id, False)

def send_data_quality(id, status):

    # Get current date and time
    current_date = (datetime.now()).strftime('%Y-%m-%d')
    
    try:
        # Connect to the MySQL database
        cursor, connection = get_database_connection()

        # Insert error log into the data_quality_logs table
        insert_query = """
        INSERT INTO prod.posting_status (siteid, posting_date, swagger_status)
        VALUES (%s,%s,%s)
        """
        data = (id, current_date, status)  # False indicates an error occurred

        cursor.execute(insert_query, data)
        connection.commit()

        print(f"Inserting into DQ log for ID {id} at {current_date}")

    except mysql.connector.Error as db_err:
        print(f"Failed to insert into DQ log: {db_err}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    execute()