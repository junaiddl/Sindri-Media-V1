import mysql.connector
import json
import pandas as pd
import os
import requests 
from combine_visit_queries import combine_queries
from combine_stats_queries import combine_stats_queries
from combine_click_chart_queries import click_chart_queries
from combine_pageviews_queries import pagviews_queries
from visits_mailing_queries import visits_mailing_queries
from stats_mailing_queries import stats_mailing_queries
from pageviews_mailing import pageviews_mailing
from click_chart_mailing import click_chart_mailing
from report import report

OUTPUT_DIR = "execute_json"
base_url = "https://staging.sindri.media/api/v1/"

def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
        )
        cursor = connection.cursor()
        return cursor, connection           
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)

def authenticate():
    try:
        url = 'https://staging.sindri.media/strapi/api/auth/local'
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

def post(token,url,json_data_list):
    global status
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
                    status = False
                    print(f'Error: {response.status_code} - {response.text}')
            else:
                print(f'Error: {response.status_code} - {response.text}')
    except Exception as e:
        print(e)

def ensure_output_dir():
    """Create output folder if not exists"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def normalize_db_json(rows):
    """
    Normalize DB results into a JSON-serializable object
    Handles:
    - [(json_string,)]
    - [json_string]
    - [(dict,)]
    - [(list,)]
    - multiple rows of JSON
    """

    if not rows:
        return []

    # Case 1: single row
    if len(rows) == 1:

        row = rows[0]

        # Row is tuple → take first column
        if isinstance(row, tuple):
            value = row[0]
        else:
            value = row

        # Already JSON-like
        if isinstance(value, (dict, list)):
            return value

        # JSON string
        if isinstance(value, str):
            return json.loads(value)

        return value

    # Case 2: multiple rows
    result = []

    for row in rows:
        value = row[0] if isinstance(row, tuple) else row

        if isinstance(value, str):
            value = json.loads(value)

        result.append(value)

    return result

def execute_and_save( query, filename,url):
    cursor, connection = get_database_connection()
    token = authenticate()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print(f"No data returned for {filename}")
            return None

        result_json = normalize_db_json(rows)
        json_date_list =[]
        json_date_list.append(result_json)
        ref_url = base_url + url
        print(ref_url)
        post(token,ref_url,json_date_list)

        file_path = os.path.join(OUTPUT_DIR, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)

        return file_path

    finally:
        cursor.close() 
        connection.close()  # 🔑 VERY IMPORTANT


def execute_query():
    code = 14.0

    ensure_output_dir()
    try:
        stats_month, stats_ytd = combine_stats_queries()
        visit_day, visit_month, visit_ytd = combine_queries()
        pageview_month, pageview_ytd = pagviews_queries()
        click_chart_month, click_chart_ytd = click_chart_queries()
        report_query = report()
        execute_and_save( report_query, "report.json", f"set-widget/newsletter::reports/")
        execute_and_save( pageview_month, "pageview_month.json", f"set-widget/newsletter::engagement-pageviews::chart::{code}::month/")
        execute_and_save( pageview_ytd, "pageview_ytd.json", f"set-widget/newsletter::engagement-pageviews::chart::{code}::ytd/")

        execute_and_save( stats_month, "stats_month.json",f"set-widget/newsletter::stats::object::{code}::month/")
        execute_and_save( stats_ytd, "stats_ytd.json",f"set-widget/newsletter::stats::object::{code}::ytd/")

        execute_and_save( visit_day, "visit_day.json", f"set-widget/newsletter::visits::chart::{code}::day/")
        execute_and_save( visit_month, "visit_month.json",f"set-widget/newsletter::visits::chart::{code}::week/")
        execute_and_save( visit_ytd, "visit_ytd.json",f"set-widget/newsletter::visits::chart::{code}::month/")

        execute_and_save( click_chart_month, "click_chart_month.json", f"set-widget/newsletter::engagement-clicks::chart::{code}::month/")
        execute_and_save( click_chart_ytd, "click_chart_ytd.json", f"set-widget/newsletter::engagement-clicks::chart::{code}::ytd/")       
    except Exception as e:
        print(f"An error occurred: {e}")



def list_queries(list_id,code):
    ensure_output_dir()
    
    try:
        pageviews_month_query, pageviews_ytd_query = pageviews_mailing(list_id)
        click_month_query, click_ytd_query = click_chart_mailing(list_id)
        stats_month_query, stats_ytd_query = stats_mailing_queries(list_id)
        visits_day_query, visits_month_query, visits_ytd_query = visits_mailing_queries(list_id)

        execute_and_save( pageviews_month_query, f"pageviews_month_{list_id}.json",f"set-widget/newsletter::engagement-pageviews::chart::{code}::month/")
        execute_and_save( pageviews_ytd_query, f"pageviews_ytd_{list_id}.json",f"set-widget/newsletter::engagement-pageviews::chart::{code}::ytd/")

        execute_and_save( click_month_query, f"click_chart_month_{list_id}.json",f"set-widget/newsletter::engagement-clicks::chart::{code}::month/")
        execute_and_save( click_ytd_query, f"click_chart_ytd_{list_id}.json",f"set-widget/newsletter::engagement-clicks::chart::{code}::ytd/")  

      
        execute_and_save( stats_month_query, f"stats_month_{list_id}.json",f"set-widget/newsletter::stats::object::{code}::month/")
        execute_and_save( stats_ytd_query, f"stats_ytd_{list_id}.json",f"set-widget/newsletter::stats::object::{code}::ytd/")

        execute_and_save( visits_day_query, f"visits_day_{list_id}.json",f"set-widget/newsletter::visits::chart::{code}::day/")
        execute_and_save( visits_month_query, f"visits_month_{list_id}.json",f"set-widget/newsletter::visits::chart::{code}::week/")
        execute_and_save( visits_ytd_query, f"visits_ytd_{list_id}.json",f"set-widget/newsletter::visits::chart::{code}::month/")

        

           
    except Exception as e:
        print(f"An error occurred: {e}")
   


def final_execute():
    execute_query()
    # print("Executing mailing list queries...")
    list_ids = (1, 2, 3)
    for list_id in list_ids:
        if list_id == 1:
            code = 14.1 
        elif list_id == 2:
            code = 14.2
        elif list_id == 3:
            code = 14.3
        list_queries(list_id, code)

if __name__ == "__main__":
    final_execute()
    print("Execution completed.")

    
    
    
   