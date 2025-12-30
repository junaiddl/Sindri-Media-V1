import os
import mysql.connector
import subprocess
import time

from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)
# Establish a connection to the MySQL database
def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT")),
            charset='utf8mb4',  # Ensure proper handling of all Unicode characters
            collation='utf8mb4_unicode_ci'
        )
        cursor = connection.cursor()
        cursor.execute("SET NAMES utf8mb4;")
        cursor.execute("SET CHARACTER SET utf8mb4;")
        cursor.execute("SET character_set_connection=utf8mb4;")
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None

# Function to check if a stored procedure exists
def procedure_exists(cursor, procedure_name):
    cursor.execute(f"SHOW PROCEDURE STATUS LIKE '{procedure_name}'")
    return cursor.fetchone() is not None

# Function to drop a stored procedure if it exists
def drop_procedure(cursor, connection, procedure_name):
    try:
        cursor.execute(f"DROP PROCEDURE IF EXISTS `{procedure_name}`")
        connection.commit()
        print(f"Procedure {procedure_name} dropped successfully.")
    except mysql.connector.Error as error:
        print(f"Error dropping procedure {procedure_name}: {error}")

# Function to create a stored procedure
def create_procedure(cursor, connection, procedure_name, sql_content):
    try:
        cursor.execute(sql_content)
        connection.commit()
        print(f"Procedure {procedure_name} created successfully.")
    except mysql.connector.Error as error:
        print(f"Error creating procedure {procedure_name}: {error}")

def run_dbt():
    # Directory containing the virtual environment
    venv_dir = os.path.join(os.path.dirname(__file__), '..', 'dbt-env', 'Scripts')
    activate_script = os.path.join(venv_dir, 'activate')

    # Commands to run
    dbt_clean_command = 'dbt clean'
    dbt_run_command = 'dbt run'

    # Activate the virtual environment and run the dbt commands
    command = f'cmd /c "cd /d {os.path.dirname(__file__)} && {activate_script} && {dbt_clean_command} && {dbt_run_command}"'

    try:
        # Run the command and wait for it to complete
        subprocess.run(command, shell=True, check=True)
        print("dbt clean and dbt run executed successfully.")
    except subprocess.CalledProcessError as error:
        print(f"Error running dbt: {error}")
        # Continue execution even if there is an error
    return True

def main(flag):
    # Directory containing SQL files
    current_dir = os.path.dirname(__file__)
    sql_directory = os.path.join(current_dir, 'target', 'compiled', 'dbt_test', 'models', 'example')

    # Check the flag value
    if flag:
        run_dbt()

        # Wait a bit to ensure the target directory is created (adjust as needed)
        time.sleep(10)

    # Ensure the directory exists after running dbt commands
    if not os.path.exists(sql_directory):
        print(f"Directory {sql_directory} does not exist.")
        return

    # Get the database connection
    cursor, connection = get_database_connection()
    if not cursor or not connection:
        return

    # List all SQL files in the directory
    files = os.listdir(sql_directory)

    for file in files:
        if file.endswith('.sql'):
            # Read the content of the SQL file
            file_path = os.path.join(sql_directory, file)
            with open(file_path, 'r', encoding='utf-8') as sql_file:
                sql_content = sql_file.read()

            procedure_name = os.path.splitext(file)[0]
            print(procedure_name)
            drop_procedure(cursor, connection, procedure_name)
            create_procedure(cursor, connection, procedure_name, sql_content)

    # Close the database connection
    cursor.close()
    connection.close()

if __name__ == "__main__":
    flag = False  # Set the flag value as needed or pass it from the command line or environment
    main(flag)
