import mysql.connector
import os
import shutil
import subprocess


site_number = [21]

conn = mysql.connector.connect(
host='host',
user='Site',
password='password',
database='prod',
port = '0000',
charset='utf8mb4'
)





def execute_sql_file(cursor, file_path):
    """Execute SQL statements from a file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_script = file.read()
        # print(sql_script)
        for result in cursor.execute(sql_script, multi=True):
            if result.with_rows:
                print(result.fetchall())

def delete_all_files_in_directory(directory):
    """Delete all files in the specified directory."""
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                print(f"Deleted file: {file_path}")
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

def copy_all_files_to_directory(src_directory, dest_directory):
    """Copy all files from the source directory to the destination directory."""
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)
    for file_name in os.listdir(src_directory):
        src_file_path = os.path.join(src_directory, file_name)
        dest_file_path = os.path.join(dest_directory, file_name)
        try:
            if os.path.isfile(src_file_path):
                shutil.copy(src_file_path, dest_file_path)
                print(f"Copied file: {src_file_path} to {dest_file_path}")
        except Exception as e:
            print(f"Failed to copy {src_file_path}. Reason: {e}")

def run_powershell_command(command):
    """Run a PowerShell command."""
    try:
        result = subprocess.run(["powershell", "-Command", command], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Command output:", result.stdout)
        print("Command error (if any):", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        print(f"Command output: {e.output}")
        print(f"Command stderr: {e.stderr}")

def replace_file_content(source_file, destination_file):
    """Replace the content of destination_file with the content of source_file."""
    try:
        shutil.copyfile(source_file, destination_file)
        print(f"Successfully replaced contents of {destination_file} with {source_file}.")
    except IOError as e:
        print(f"Unable to copy file. {e}")



def main():
    for site in site_number:
        # File paths
        source_file = f"C:\\Users\\Admin\\Desktop\\Sindri_Media\\Data_Built_Tool\\Yaml_Configs\\dbt_project_{site}.yml"
        destination_file =  r"C:\Users\Admin\Desktop\Sindri_Media\Data_Built_Tool\dbt_project\dbt_project.yml"
        # Replace file content
        replace_file_content(source_file, destination_file)
        # Example usage:
        command = r"cd 'C:\Users\Admin\Desktop\Sindri_Media\Data_Built_Tool\dbt_project'; dbt --no-populate-cache compile"

        run_powershell_command(command)

        # Delete all files in the specified directory
        models_directory = r"C:\Users\Admin\Desktop\Sindri_Media\Data_Built_Tool\dbt_project\target\compiled\dbt_project\models"
        backup_directory = f"C:\\Users\\Admin\\Desktop\\Sindri_Media\\Data_Built_Tool\\Site{site}"

        # Copy all files to the backup directory
        print("Copying files to backup directory...")

        copy_all_files_to_directory(models_directory, backup_directory)
        copy_all_files_to_directory(models_directory + r"\overview", backup_directory)
        copy_all_files_to_directory(models_directory + r"\tendency", backup_directory)
        copy_all_files_to_directory(models_directory + r"\traffic", backup_directory)
        copy_all_files_to_directory(models_directory + r"\exploration", backup_directory)
        copy_all_files_to_directory(models_directory + r"\article_eval", backup_directory)
        copy_all_files_to_directory(models_directory + r"\bigview", backup_directory)

        delete_all_files_in_directory(models_directory)
        delete_all_files_in_directory(models_directory + r"\overview")
        delete_all_files_in_directory(models_directory + r"\tendency")
        delete_all_files_in_directory(models_directory + r"\traffic")
        delete_all_files_in_directory(models_directory + r"\exploration")
        delete_all_files_in_directory(models_directory + r"\article_eval")
        delete_all_files_in_directory(models_directory + r"\bigview")

        # Copy YAML file to the backup directory
        backup_yaml_file = os.path.join(backup_directory, f"dbt_project_{site}.yml")
        shutil.copyfile(source_file, backup_yaml_file)
        print(f"Copied YAML file to backup directory: {backup_yaml_file}")

    exit()

    site_to_test = 14

    # Connect to the MySQL database
    cursor = conn.cursor()

    try:
        # Execute SQL files from dynamic folder
        print("Dropping Existing Procedures...")
        file_path = f"C:\\Users\\Admin\\Desktop\\Sindri (Kasper)\\DBT_Testing\\Site{site_to_test}\\drop.sql"
        execute_sql_file(cursor, file_path)
        conn.commit()

        # Execute SQL files from dynamic folder
        print("Creating procedures...")
        dynamic_folder = f"C:\\Users\\Admin\\Desktop\\Sindri (Kasper)\\DBT_Testing\\Site{site_to_test}"
        for file_name in os.listdir(dynamic_folder):
            if(file_name == 'drop.sql'):
                continue
            file_path = os.path.join(dynamic_folder, file_name)
            if file_path.endswith('.sql'):
                print(f"Executing {file_name} from dynamic folder...")
                execute_sql_file(cursor, file_path)
                conn.commit()

        print("Stored procedures have been managed successfully.")
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    # post to site 2
    # post_cmd = r"& 'C:/Users/Admin/AppData/Local/Programs/Python/Python311/python.exe' 'c:/Users/Admin/Desktop/Sindri (Kasper)/DBT_Testing/testing_dbt_post_2.py'"
    # run_powershell_command(post_cmd)

if __name__ == "__main__":
    main()