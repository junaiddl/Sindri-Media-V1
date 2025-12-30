import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
import html
import os

def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
               host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            port = int(os.getenv("DB_PORT")),
        )
        logging.info('Database Connected...')
        return connection
    except mysql.connector.Error as error:
        logging.error("Error connecting to MySQL database: %s", error)
        raise

yesterday = datetime.today().date() - timedelta(days=7)
yesterday_str = yesterday.strftime('%Y-%m-%d')

fetching_end = datetime.today().date() - timedelta(days=1)
fetching_end_str = fetching_end.strftime('%Y-%m-%d')

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')

def insert_into_pre_stage(cursor, connection, data_list):
    print("Starting To Insert Data Into Pre_Stage")
    for data in data_list:
        print(data)

    insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (id,Title,Date,Link,Categories,Tags,Status,Modified,siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
            """
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info('Data insertion successful in pre_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into pre_stage MySQL database: %s", e)
        connection.rollback()
        raise


def fetch_all_labrador_data(api_url, siteid):
    page = 1
    continue_fetching = True
    
    while page<2 and continue_fetching:
        params = {
            'after': f"""{yesterday}T00:00:00""",
            'before':f"""{today}T00:00:00""",
            'page': page,
            'per_page': 100  
        }
        
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            data = response.json()

            if not data:  # If the data is empty, break the loop
                break

            connection = get_database_connection()
            cursor = connection.cursor()

            # Extracting only the required fields
            extracted_data = []
            
            for article in data:
                postid = article.get("id", None)
                title = article.get('title', None)
                if title:
                    title_enc = title['rendered']
                    title =html.unescape(title_enc)
                date = article.get('date', None)
                if date and isinstance(date, str):
                    result_datetime = parse(date)
                    date = result_datetime.strftime("%Y-%m-%d")
                    if(date < yesterday_str) or (date > fetching_end_str):
                        continue
                modified = article.get('modified', None)
                if modified and isinstance(modified, str):
                    result_datetime = parse(modified)
                    modified = result_datetime.strftime("%Y-%m-%d")
                url = article.get('link', None)

                cleaned_url = url.rstrip('/')

                status = article.get('status', None)
                categories = article.get('type', None)
                tag = article.get('subject', None)
                if isinstance(tag, list) and tag:
                    tag = tag[0]
                else:
                    tag = ""

                extracted_data.append((postid, title, date, cleaned_url, categories, tag, status, modified, siteid))

            insert_into_pre_stage(cursor, connection, extracted_data)

            # If the length of the data is less than 10, set the flag to stop further fetching
            print(page,len(data))
            if len(data) < 100:
                continue_fetching = False

            page += 1  # Increment the page number for the next iteration

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except:
            print("No Ingestion!")

def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        connection.commit()
    except Exception as e:
        print(e)

def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags,status, Modified,siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.status,cast(s.Modified as date), s.siteid
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
            WHERE d.ID IS NULL AND d.Date is null AND s.siteid = {siteid} AND s.ID = {postid} AND s.Date = '{date}';
    """
    print(insert_query)
    cursor.execute(insert_query)
    connection.commit()

def start_execution():
    siteid = 14
    labrador_api_url = ["https://fagbladet3f.dk/wp-json/wp/v2/posts","https://fagbladet3f.dk/wp-json/wp/v2/news_overview",
                       "https://fagbladet3f.dk/wp-json/wp/v2/guide","https://fagbladet3f.dk/wp-json/wp/v2/digidoc",
                       "https://fagbladet3f.dk/wp-json/wp/v2/debate"]
   
    
    print("Truncating Data For Site 14 In Pre_Stage")
    truncate(14)

    # for each API Url, fetch data and insert it into pre_stage
    print("Starting Data Ingestion Into Pre_Stage")
    for api in labrador_api_url:
        print("API: ", api)
        fetch_all_labrador_data(api, siteid)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()