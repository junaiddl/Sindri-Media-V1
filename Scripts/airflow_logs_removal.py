from fabric import Connection
from paramiko.ssh_exception import AuthenticationException, SSHException, NoValidConnectionsError
from datetime import datetime, timedelta
import os

def connect_to_ec2(hostname, username, key_filename):
    try:
        c = Connection(host=hostname, user=username, connect_kwargs={"key_filename": key_filename})
        c.open()
        print("Connection to {} successful.".format(hostname))
        return c
    except AuthenticationException:
        print("Authentication failed while connecting to {}.".format(hostname))
    except SSHException as e:
        print("SSH connection error to {}: {}".format(hostname, str(e)))
    except NoValidConnectionsError as e:
        print("No valid connections to {}: {}".format(hostname, str(e)))
    except Exception as e:
        print("An error occurred while connecting to {}: {}".format(hostname, str(e)))
    return None

def delete_old_logs_for_dag(connection, dag_name, folders_to_delete):
    try:
        if folders_to_delete[0] != '' and len(folders_to_delete) != 1:
            if folders_to_delete:
                # Check if the folders to be deleted exist and are not empty
                for folder in folders_to_delete:
                    folder_exists = connection.run(f'test -d /root/airflow/logs/{dag_name}/{folder} && echo "True" || echo "False"', hide=True).stdout.strip()
                    if folder_exists == "True":
                        connection.run(f'rm -rf /root/airflow/logs/{dag_name}/{folder}')
                        print(f"Specified log folders for DAG '{dag_name}' have been deleted.")
                    else:
                        print(f"No log folders to delete for DAG '{dag_name}' older than seven days.")
            else:
                print(f"No log folders to delete for DAG '{dag_name}'.")
        else :
             print(f"No log folders to delete for DAG '{dag_name},{folders_to_delete}'.")
    except Exception as e:
        print(f"An error occurred while deleting log folders for DAG '{dag_name}': {str(e)}")

hostname = '51.15.222.227'
username = 'root'

key_filename = os.path.join(os.path.dirname(__file__), '..', 'Keys', 'ssh-key.pem')

connection = connect_to_ec2(hostname, username, key_filename)


def main_function():
    if connection:
        try:
            folders_result = connection.run('ls -l -d /root/airflow/logs/*/', hide=True)
            print("Folders in /root/airflow/logs/:")
            print(folders_result.stdout)
            print('log checking')
            dag_logs_count = {}
            dag_logs_folders_before_seven_days = {}

            for folder in folders_result.stdout.splitlines():
                dag_name = folder.split('/')[-2]
                logs_count = connection.run(f'ls -l /root/airflow/logs/{dag_name}/ | wc -l', hide=True).stdout.strip()
                dag_logs_count[dag_name] = int(logs_count) - 1

                log_folders_before_seven_days = connection.run(
                    f'find /root/airflow/logs/{dag_name}/ -type d -mtime +6 -printf "%f\n"',
                    hide=True
                ).stdout.strip().split('\n')
                dag_logs_folders_before_seven_days[dag_name] = log_folders_before_seven_days

            print("DAGs and their log counts:")
            print(dag_logs_count)
           

            print("DAGs and their log folders created before seven days:")

            # dag_name_to_delete_logs = 'dag_id=cms_14_dag'
            # /root/airflow/logs/dag_id=cms_14_dag

            # if dag_name_to_delete_logs in dag_logs_folders_before_seven_days and len(dag_logs_folders_before_seven_days[dag_name_to_delete_logs]) !=1 :
            
            #     delete_old_logs_for_dag(connection, dag_name_to_delete_logs, dag_logs_folders_before_seven_days[dag_name_to_delete_logs])
            # else:
            #     print(f"No log folders to delete for DAG '{dag_name_to_delete_logs}'.")

        # exit()
        # # /root/airflow/logs/dag_id=traffic_channel_data_post_site_4

        # Iterate over the DAGs with log folders older than seven days
            for dag_name, folders_to_delete in dag_logs_folders_before_seven_days.items():

                print(dag_name)
                if folders_to_delete and  folders_to_delete[0] != '' and len(folders_to_delete) != 1 :
                    delete_old_logs_for_dag(connection, dag_name, folders_to_delete)
                else:
                    print(f"No log folders to delete for DAG '{dag_name}'.")

        except Exception as e:
            print("An error occurred while executing commands: {}".format(str(e)))
        finally:
            connection.close()
    else:
        print("Connection to {} failed. Unable to execute commands.".format(hostname))