import os
import yaml
import mysql.connector
import json
import requests
import logging
import time
import subprocess
import traceback
import ssl
import smtplib
import mailst
import mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from mysql.connector import Error as MySQLError
from datetime import datetime, timedelta

from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)
def authenticate():
    try:
        url = 'https://sindri.media/strapi/api/auth/local'
        payload = {
            'identifier': 'import@dashboardapp.com',
            'password': 'JSyzkFtR9Cw5v5t'
        }
        auth_response = requests.post(url, data=payload)
        
        # Assuming the JWT token is returned in the 'token' field of the response JSON
        token = auth_response.json().get('jwt')
        return token
        
    except requests.exceptions.RequestException as req_err:
        print("Request error occurred:", req_err)
        return None
    except Exception as ex:
        print("An unexpected exception occurred:", ex)
        return None
        
def get_database_connection():
    # # Establish a connection to the MySQL database
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = connection.cursor()
        return cursor, connection
        


    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)



        
def post(token,url,json_data_list):
    while token is None:  # Keep trying until a valid token is obtained
        print("Invalid or None token. Re-authenticating...")
        token = authenticate()
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        print('len: ',len(json_data_list))
        c = 0
        for json_data in json_data_list:
            response = requests.post(url, headers=headers, json=json_data)
            if response.status_code == 200:
                c=c+1
                print(response.text,c)
            elif response.status_code == 401:  # Unauthorized, possibly due to invalid token
                print("Unauthorized access. Refreshing token.")
                token = authenticate()
                while token is None:  # Keep trying until a valid token is obtained
                    print("Invalid or None token. Re-authenticating...")
                    token = authenticate()
                headers = {
                                'Authorization': f'Bearer {token}',
                                'Content-Type': 'application/json'}
                response = requests.post(url, headers=headers, json=json_data)
                if response.status_code == 200:
                    c = c + 1
                    print(response.text, c)
                else:
                    print(f'Error: {response.status_code} - {response.text}')
            else:
                print(f'Error: {response.status_code} - {response.text}')
    except Exception as e:
        print(e)

def compare_json_format(json1, json2):
    try:
        json2 = json.dumps(json2)
        # Load JSON strings into Python dictionaries
        data1 = json.loads(json1)
        data2 = json.loads(json2)

        # Extract keys from the dictionaries
        keys1 = set(data1.keys())
        keys2 = set(data2.keys())

        # Compare the keys
        if keys1 == keys2:
            logging.info("=====================The JSON formats have the same keys========================")
            return True
        else:
            logging.info("The JSON formats have different keys.")
            return False
    except Exception as e:
        print(e)
        return False
    
            
def server_resour():
    # List of commands to run
    commands = [
        'df -h',
        'free -m',
        'top -b -n 1 | grep Cpu'
    ]
    res_list =[]

    for command in commands:
        try:
            output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT, text=True)
            res_list.append(output)
        except subprocess.CalledProcessError as e:
            res_list.append(e.output)
    return res_list

def swagger_status(siteid):
    yesterday = datetime.today().date() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    cursor,connection = get_database_connection()
    status =True
    delete_query=f"""delete from  prod.data_quality_logs where siteid ={siteid} and check_name ='posting' """
    insert_query = """
        INSERT INTO prod.data_quality_logs(siteid, check_name, status, inserted_at) 
        VALUES (%s, 'posting', %s, %s)"""
    try:
        cursor.execute(delete_query)
        connection.commit()
        cursor.execute(insert_query,(siteid,status,yesterday_str))
        connection.commit()
        print('Data insertion successful in prod')
    except MySQLError as e:
        logging.error("Error inserting data into prod MySQL database: %s", e)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def start_execution():
    
    # Select the  values of the credentials column
    cursor,connection = get_database_connection()
    select_query = f"select * from prod.credentials_detail where status ='active' and siteid in (3,10);"
    cursor.execute(select_query)
    cred = cursor.fetchall()
    print('result',cred)
    cursor.close()
    connection.close()
    for row in cred:
        siteid = row[1]
        yaml_DQ(siteid)


def yaml_DQ(siteid):
    try:
        # ---------------------YAML FILE READ---------------------
        yaml_file_path = r'/root/airflow/dags/Data_Quality.yaml'
        try:
            with open(yaml_file_path, 'r') as file:
                config = yaml.safe_load(file)
                print("YAML file opened successfully.")
        except FileNotFoundError:
            print("YAML file not found.")
            config = None
        except Exception as e:
            print("Error opening YAML file:", e)
            config = None

        # -----------------Data Quality start ---------------------
        new_where_clause = str(siteid)
        if config is not None:
            dq_checks = config.get('dq_checks')

            if dq_checks and isinstance(dq_checks, list):
                count = 0
                for dq_check in dq_checks:

                    check_number = ''.join(filter(str.isdigit, dq_check.get('name')))
                    if not check_number or int(check_number) == siteid:
                        # get the name, query in yaml
                        name = dq_check.get('name')
                        sql_query = dq_check.get('sql_query')
                        # replace the where cluse
                        sql_query = sql_query.replace("siteid_value", new_where_clause)
                        stop_on_failure = dq_check.get('stop_on_failure', False)
                        target = 0  # Initialize target value
                        if sql_query:
                            try:
                                try:  
                                    cursor,connection = get_database_connection()
                                    cursor.execute(sql_query)
                                    print(sql_query)
                                    result = cursor.fetchone()
                                    cursor.close()
                                    connection.close()
                                    result_value =0
                                except Exception as e:
                                    print(e)
                                    cursor,connection = get_database_connection()
                                    cursor.execute(sql_query)
                                    print(sql_query)
                                    result = cursor.fetchone()
                                    cursor.close()
                                    connection.close()
                                    result_value =0

                                if result is not None and result[0] is not None and result[0] > 0:
                                    print(f"Query result for '{name}': {result[0]}")
                                    result_value = True if result[0] > 0 and stop_on_failure else False
                                    try:
                                        count =0
                                        swagger = "Swagger Data Post"  # Modify this with your actual Swagger link
                                        # Insert the result into the prod.dq_checks table
                                        subject = f'[ISSUE][DQ_failed][DQ_name : {name}][Siteid : {siteid}] Data Quality'
                                        message = f"Hello ! "
                                        message = str(message)
                                        email_template = f"""
                                            <!DOCTYPE html>
                                            <html>
                                            <head>
                                                <style>
                                                    .failed-list {{
                                                        border-collapse: collapse;
                                                        width: 100%;
                                                        border: 1px solid black;
                                                    }}

                                                    .failed-list th, .failed-list td {{
                                                        border: 1px solid black;
                                                        padding: 8px;
                                                        text-align: left;
                                                    }}

                                                    .failed-list th {{
                                                        background-color: #f2f2f2;
                                                    }}
                                                </style>
                                            </head>
                                            <body>
                                                <div class="email-container">
                                                    <div class="header">Data Quality Check Report: Failed Entries</div>

                                                    <div class="details">
                                                        Dear Team,
                                                        <p>We are writing to inform you about recent data quality check failures that require your attention. Our data quality monitoring process has identified issues in the following entries: An important update the Data Quality check {name} for Site ID {siteid} has failed. Immediate action is needed to resolve this issue.</p>
                                                    </div>

                                                    <table class="failed-list">
                                                        <tr>
                                                            <th>siteId</th>
                                                            <th>name</th>
                                                            <th>value</th>
                                                            <th>target</th>
                                                            <th>result</th>
                                                            <th>Stop on failure</th>
                                                            <th>source</th>

                                                        </tr>
                                                        <tr>
                                                            <td><strong>{siteid}</strong></td>
                                                            <td>{name}</td>
                                                            <td>{result[0]}</td>
                                                            <td>{target}</td>
                                                            <td>{result_value}</td>
                                                            <td>{stop_on_failure}</td>
                                                            <td>{swagger}</td>

                                                        </tr>

                                                        <!-- Add more failed entries as needed -->
                                                    </table>
                                                    <br>

                                                    <hr>
                                                    <p><strong><b>Server Resources</b></strong></p>
                                                    <table class="failed-list">


                                                        <!-- Add more failed entries as needed -->
                                                    </table>

                                                    <div class="details">
                                                        <p>We kindly request that you review and address these issues promptly to ensure the accuracy and reliability of our data. If you have any questions or need further assistance, please don't hesitate to reach out to our support team.</p>

                                                        <p>Thank you for your attention to this matter.</p>

                                                        <p>Sincerely,
                                                        <br>[Gulraiz]
                                                        <br>[Dotlabs]
                                                        <br>[https://sindri.media/]</p>
                                                    </div>
                                                </div>
                                            </body>
                                            </html>
                                            """
                                        insert_query = """
                                        INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        """
                                        values = (result[0], name, stop_on_failure, target, result_value, swagger,siteid)
                                        try:
                                            cursor,connection = get_database_connection()
                                            cursor.execute(insert_query, values)
                                            connection.commit()
                                            connection.close()
                                            try:
                                                mail.mailConnection(subject, message,email_template) 
                                            except Exception as e:
                                                mail.mailConnection(subject, message,email_template)
                                                print(e)

                                        except Exception as e:
                                            print(e)
                                            cursor,connection = get_database_connection()
                                            cursor.execute(insert_query, values)
                                            connection.commit()
                                            connection.close()
                                            try:
                                                mail.mailConnection(subject, message,email_template) 
                                            except Exception as e:
                                                mail.mailConnection(subject, message,email_template)
                                                print(e)
                                        if stop_on_failure:
                                            print(f"Aborting further checks due to failure in '{name}'.")
                                            d = "DQ is fail"
                                            # return d
                                    except Exception as e:
                                        print(e)
                                        # Stop checking other jobs
                            except Exception as e:
                                print(e)
            if count == 0:               
                insert_query = """
                INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                values = (0, "Swagger Post, DQ Check Successful", 0, 0, 0,  "Swagger Data DQ complete",siteid)
                try:

                    cursor,connection = get_database_connection()
                    cursor.execute(insert_query, values)
                    connection.commit()
                    connection.close()

                except Exception as e:
                    print(e)
                    cursor,connection = get_database_connection()
                    cursor.execute(insert_query, values)
                    connection.commit()
                    connection.close()
                # try:
                #     logging.info("Site Archive Post")
                #     cursor,connection = get_database_connection()
                #     select_query = f"select count(status) as status from prod.data_quality_logs where siteid ={siteid}  and check_name not in('card_table_count_dq','tendancy_count','posting') and status =0"
                #     cursor.execute(select_query)
                #     status= cursor.fetchone()[0]
                #     if status == 0:
                truncate_data_quality(siteid)
                start_endpoint_to_post(siteid)
                send_data_quality(siteid, True)
                # except Exception as e:
                #     print("DQ check error:",siteid)
                
        else:
            results = "yaml file is not opened"
            print('yaml file is not opened')
            

    except Exception as e:
        print(e)
def start_endpoint_to_post(siteid):
    try:
        
        print('start_data')
        subject = f"[COMPLETED]Data Posted Completed via Swagger: Site ID {siteid}"
        message = f"Sindri.media"
        message = str(message)
         
        # ---------------------ENDPOINT and Function name get-------------------
        
        select_query = f"""call prod.SelectINputBasedOnSiteId({siteid});"""
        
        #  database connection handle
        try:
            # Select the  values of the credentials column
            cursor,connection = get_database_connection()
            cursor.execute(select_query)
            endp = cursor.fetchall()
        except mysql.connector.errors.OperationalError as op_err:
            if "is locked" in str(op_err):
                print("Database is busy. Please try again later.")
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
            elif "Can't connect" in str(op_err):
                print("Unable to connect to the database.")
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
            else:
                print("An unexpected operational error occurred:", op_err)
                cursor,connection = get_database_connection()
                cursor.execute(select_query)
                endp = cursor.fetchall()
        except mysql.connector.errors.IntegrityError as int_err:
            print("Integrity error occurred:", int_err)
        except mysql.connector.errors.ProgrammingError as prog_err:
            print("Programming error occurred:", prog_err)
        except mysql.connector.errors.InterfaceError as intf_err:
            print("Interface error occurred:", intf_err)
        except Exception as e:
            print(e)
            cursor,connection = get_database_connection()
            cursor.execute(select_query)
            endp = cursor.fetchall()
        
        
        connection.close()
        cursor.close()
        print('siteid : ',siteid)
        token = authenticate()
        
        
            # ------------------- Query starting-----------
        for i in endp:
            try:
                json_data = 0
                json_data_list = []
                # print('start')
                req_json_format = i[3]
                parameter = str(i[2])
                print('\n',i[0])
                label_val = None
                hint_val = None
                event_action_val = None
                series = None
                category = None
                DefaultTitle = None
                ty = False
                # -------------- label and hints get in the json input column---------
                if i[7] is not None and len(i[7]) > 0:
                    data_dict = json.loads(i[7])
                    word_to_find = "table"
                    # Check if the word is present in the string
                    if i[1].lower().find(word_to_find.lower()) != -1:
                        if 'label' in data_dict:
                            label_val = data_dict["label"]
                        ty = True
                    else:
                        if 'label' in data_dict:
                            label_val = data_dict["label"]
                        if 'hint' in data_dict:
                            hint_val = data_dict["hint"]


                # -------------- query specific input ---------
                if i[5] is not None and len(i[5]) > 0:
                    data_dict = json.loads(i[5])
                    for key, val in data_dict.items():
                        if key == "event_action":
                            print(val)
                            event_action_val = val
                            print('event_action_val ', event_action_val)
                        elif key == "series":
                            series = val
                            series_val1 = series[0]
                            series_val2 = series[1]
                        elif key == "category":
                            category = val
                        elif "DefaultTitle"  in data_dict:
                            DefaultTitle = data_dict["DefaultTitle"]
                            Additional1 = data_dict["Additional1"]
                            Additional2 = data_dict["Additional2"]
                            dtitle = data_dict["title"][0]
                            title1 = data_dict["title"][1]
                            title2 = data_dict["title"][2]
                        else:
                            print('No find')
                # ---------------query , SP , Parameter , input
                
                if (event_action_val is not None) and (category is not None):
                    query =  f"call prod.{i[1]}({siteid}, '''{label_val}''' ,  '''{hint_val}''' ,'{category}','''{event_action_val}''');"
                
                elif (event_action_val is not None) and (series is not None):
                    query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}', '{series_val1}' , '{series_val2}');"
                elif (label_val is not None) and  (DefaultTitle is not None) and (event_action_val is not None):
                    query = f"call prod.{i[1]}({siteid}, '''{label_val}''' , '''{hint_val}''', '{DefaultTitle}', '{Additional1}','{Additional2}','{dtitle}','{title1}','{title2}','''{event_action_val}''');"
                # Check if at least one of the variables is not None
                #     elif event_action is not None or series is not None:
                    # At least one variable is not None

                    # Now, you can check which variables are not None and perform your desired action for each
                elif event_action_val is not None:
                    if (label_val is not None) and (hint_val is not None):
                        query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}' );"
                    elif ty == True:
                        query = f"call prod.{i[1]}({siteid},  '{event_action_val}', '{label_val}' );"
                    else:
                        query = f"call prod.{i[1]}({siteid},  '{event_action_val}' );"
                elif series is not None:
                    query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' ,  '{series_val1}' , '{series_val2}');"

                elif category is not None: # categories query
                    query = f"call prod.{i[1]}({siteid}, '''{label_val}''' ,  '''{hint_val}''' , '{category}' );"
                    
                elif (label_val is not None) and  (DefaultTitle is not None):
                    query = f"call prod.{i[1]}({siteid}, '''{label_val}''' , '''{hint_val}''', '{DefaultTitle}', '{Additional1}','{Additional2}','{dtitle}','{title1}','{title2}');"
                         

                
                elif ty == True:  # table queries
                    query = f"call prod.{i[1]}({siteid}, '{label_val}');"
                elif (label_val is not None) and (hint_val is not None):
                    query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}');"
                else:
                    query = f"call prod.{i[1]}({siteid});"
                print(query)
                try:
                    cursor,connection = get_database_connection()
                    cursor.execute(query)
                    # Retry parameters

                except mysql.connector.errors.OperationalError as op_err:
                    if "is locked" in str(op_err):
                        print("Database is busy. Please try again later.")
                        cursor,connection = get_database_connection()
                        cursor.execute(query)
                    elif "Can't connect" in str(op_err):
                        print("Unable to connect to the database.")
                        cursor,connection = get_database_connection()
                        cursor.execute(query)

                    else:
                        print("An unexpected operational error occurred:", op_err)
                        cursor,connection = get_database_connection()
                        cursor.execute(query)

                except mysql.connector.errors.IntegrityError as int_err:
                    print("Integrity error occurred:", int_err)
                except mysql.connector.errors.ProgrammingError as prog_err:
                    print("Programming error occurred:", prog_err)
                except mysql.connector.errors.InterfaceError as intf_err:
                    print("Interface error occurred:", intf_err)
                except Exception as e:
                    print(e)
                    cursor,connection = get_database_connection()
                    cursor.execute(query)
                results = cursor.fetchall()
                connection.close()
                cursor.close()
#                 print(results)
                if len(results)  == 0:
                    print('Blank result of this query: ',query)
                    json_data =  json.loads(i[3])
                    json_data['site'] = siteid
                    json_data_list.append(json_data)
                # Process the results
                elif None in results[0]:
                    json_data =  json.loads(i[3])
                    json_data['site'] = siteid
                    json_data_list.append(json_data)
                else:
                    # print('result: ',results)
                    for j in results:
                        json_data = json.loads(j[0])
                        json_data['site'] = siteid
                        json_data_list.append(json_data)
                    print(json_data_list)
                print(query)
                url = 'https://sindri.media' + parameter
                print(url)
                print(json_data)
                my_for = json_data
                if my_for != 0:
                    verf = compare_json_format(req_json_format,my_for)
                    verf =True
                    print('verf',verf)
                    if verf == True:
                        post(token,url,json_data_list)

                    else:
                        print(f'this query {query} of json not match in Swagger json: ',url)
                else:
                    print(f'this query {query} result is empty')
            except Exception as e:
                print(e)
        try:
            try:
                result_server = server_resour()
            except Exception as e:
                result_server =[e,e,e]
                print(e)
            # HTML email template
            email_template = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                    }}
                    .email-container {{
                        width: 80%;
                        margin: auto;
                        padding: 20px;
                        border: 1px solid #ccc;
                    }}
                    .header {{
                        font-size: 24px;
                        margin-bottom: 10px;
                    }}
                    .details {{
                        font-size: 16px;
                        margin-top: 20px;
                    }}
                    .data-section {{
                        margin-top: 15px;
                    }}
                    .data-item {{
                        margin-bottom: 5px;
                    }}
                </style>
            </head>
            <body>
                <div class="email-container">
                    <div class="header">Data Posted Completed via Swagger: Site ID {siteid}</div>
                    <div class="details">
                        <p>The following data has been successfully posted to the Swagger (sindri.media) :</p>
                    </div>
                    <div class="data-section">
                        <ul>
                            <li class="data-item"><strong>Site ID:</strong> {siteid}</li>
                            <!-- Add more data items here -->
                        </ul>
                    </div>
                    <br>

                        <hr>
                        <p><strong><b>Server Resources</b></strong></p>
                        <table class="failed-list">
                            <tr>
                                <th>Memory</th>
                                <th>Storage</th>
                                <th>CPU</th>
                            </tr>
                            <tr>
                                <td><strong>{result_server[0]}</strong></td>
                                <td>{result_server[1]}</td>
                                <td>{result_server[2]}</td>
                            </tr>

                            <!-- Add more failed entries as needed -->
                        </table>
                    <div class="details">
                        <p>Thank you. <br>
                         If you have any questions or need further assistance, please don't hesitate to contact us.</p>
                        <p>Sincerely,
                        <br>Gulraiz
                        <br>Dotlabs.ai</p>
                    </div>
                </div>
            </body>
            </html>
            """
            mail.mailConnection(subject, message,email_template)
        except Exception as e:
            print(e) 
    except Exception as e:
        print(e)
    # swagger_status(siteid)


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

# create a object of class  
def dev_etl():
    logging.info("Site Archive Post")
    start_execution()