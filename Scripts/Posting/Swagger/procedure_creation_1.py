import os
import shutil
import mysql.connector
import subprocess

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


def create_procedure(cursor, connection, procedure_name, sql_content):
    try:
        cursor.execute(sql_content)
        connection.commit()
        print(f"Procedure {procedure_name} created successfully.")
    except mysql.connector.Error as error:
        print(f"Error creating procedure {procedure_name}: {error}")

# Function to run dbt commands
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
        subprocess.run(command, shell=True, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("dbt clean and dbt run executed successfully.")
    except subprocess.CalledProcessError as error:
        print(f"running dbt")
        # Continue execution even if there is an error
    return True

def main(flag, site_id):
    if not site_id:
        print("Site ID is required.")
        return

    # Check the flag value and run dbt commands if the flag is True
    if flag:
        run_dbt()

        # Wait a bit to ensure the target directory is created (adjust as needed)

    # Directory containing SQL files
    current_dir = os.path.dirname(__file__)
    sql_directory = os.path.join(current_dir, 'target', 'compiled', 'dbt_project', 'models')

    # Check if the client folder exists, if not create it
    client_folder = os.path.join(current_dir, '..', 'client')
    if not os.path.exists(client_folder):
        os.makedirs(client_folder)

    # Check if any folder in the client folder ends with the site_id
    site_folder = None
    for folder in os.listdir(client_folder):
        if folder.endswith(site_id):
            site_folder = os.path.join(client_folder, folder)
            # If such a folder is found, delete it
            shutil.rmtree(site_folder)
            break

    # Create a new site folder
    site_folder = os.path.join(client_folder, f'site_{site_id}')
    os.makedirs(site_folder)

    # Copy the YAML file to the site folder
    yml_file = os.path.join(current_dir, 'dbt_project.yml')  # Adjust the name of the YAML file as needed
    if os.path.exists(yml_file):
        shutil.copy(yml_file, site_folder)

    # Ensure the SQL directory exists after running dbt commands
    if not os.path.exists(sql_directory):
        print(f"Directory {sql_directory} does not exist.")
        return

    # Copy all SQL files from target directory to the site folder
    for file_name in os.listdir(sql_directory):
        if file_name.endswith('.sql'):
            full_file_name = os.path.join(sql_directory, file_name)
            if os.path.isfile(full_file_name):
                shutil.copy(full_file_name, site_folder)
    print('copied to cliend specified directory')

def create_procedure_in_db(siteid):
    # Get the database connection
    cursor, connection = get_database_connection()
    if not cursor or not connection:
        return

    # List all SQL files in the site 
    # os.path.join(os.path.dirname(__file__), '..', 'DBT', 'DBT_Posting')
    site_folder = os.path.join(os.path.dirname(__file__), '..', 'Client',f'site_{siteid}')
    print(site_folder)
    # exit()
   
    files = os.listdir(site_folder)

    for file in files:
        if file.endswith('.sql'):
            # Read the content of the SQL file
            file_path = os.path.join(site_folder, file)
            with open(file_path, 'r', encoding='utf-8') as sql_file:
                sql_content = sql_file.read()

            procedure_name = os.path.splitext(file)[0]
            create_procedure(cursor, connection, procedure_name, sql_content)

    # Close the database connection
    cursor.close()
    connection.close()  

if __name__ == "__main__":
    flag = False  # Set the flag value as needed or pass it from the command line or environment
    site_id = '17'  # Replace with the actual site ID as needed or pass it from the command line or environment
    main(True, site_id)
