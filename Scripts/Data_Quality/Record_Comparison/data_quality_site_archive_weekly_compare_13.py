import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
import os

yesterday = datetime.today().date() - timedelta(days=7)
yesterday_str = yesterday.strftime('%Y-%m-%d')

fetching_end = datetime.today().date() - timedelta(days=1)
fetching_end_str = fetching_end.strftime('%Y-%m-%d')

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')

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

def insert_into_pre_stage(cursor, connection, data_list):
    print('start inserting pre_stage.site_archive_post_v2----------------')
    insert_query = """
        INSERT INTO pre_stage.site_archive_post_v2
        (siteid,id, title, date, modified, link, tags, categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info('Data insertion successful in pre_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into pre_stage MySQL database: %s", e)
        connection.rollback()
        raise

def fetch_all_labrador_data(api_url,siteid):

    params = {
        'from':yesterday,
        'to':today
    }
    count=1
    try:
            response = requests.get(api_url,params=params)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            data = response.json()
            print("Data")
            print(data)
            connection = get_database_connection()
            cursor = connection.cursor()
            #Extracting only the required fields
            extracted_data = []
            data_dict = data.get('Articles', [])
            for article in data_dict:
                postid = article.get("Id", None)
                print(postid)
                title = article.get('Title', None)
                publishdate = article.get('PublishDate', None)
                if publishdate and isinstance(publishdate, str):
                        result_datetime = parse(publishdate)
                        published_date = result_datetime.strftime("%Y-%m-%d")
                updatedate = article.get('UpdateDate', None)
                if updatedate and isinstance(updatedate, str):
                        result_datetime = parse(updatedate)
                        updated_date = result_datetime.strftime("%Y-%m-%d")
                url = article.get('Url', None)
                type = article.get('Type', None)
                tag = article.get('Tags', None)
                tags_string = ','.join(tag)
                extracted_data.append((siteid,postid, title, published_date, updated_date, url, tags_string, type))
                print("data count posted:", count) 
                count +=1

            # Insert all data into pre_stage that was fetched
            insert_into_pre_stage(cursor, connection, extracted_data)
            print(len(extracted_data))
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
# Example usage:
    
def date_correction():
    connection = get_database_connection()
    cursor = connection.cursor()
    # Correct the date values in published column
    update_query=""" UPDATE pre_stage.site_archive_post_v2
        SET date = DATE_FORMAT(Date, '%Y-%d-%m')  where date > Current_Date and siteid = 13; """
    # Correct the date values in updated column
    update_query_2=""" UPDATE pre_stage.site_archive_post_v2
        SET modified = DATE_FORMAT(Modified, '%Y-%d-%m')  where modified > Current_Date and siteid = 13; """
    try:
        cursor.execute(update_query)
        cursor.execute(update_query_2)
        connection.commit()
        logging.info('Data correction successful in pre_stage')
    except MySQLError as e:
                logging.error("Error correction data into pre_stage MySQL database: %s", e)
                connection.rollback()

def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        connection.commit()
    except Exception as e:
        print(e)

def insert_into_prod(siteid, postid, link ,date, cursor, connection):
    insert_query = f"""
    INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified,siteid)
        SELECT s.ID, s.Title, s.date, s.link, s.categories, s.Tags, "publish", s.modified, s.siteid
            FROM pre_stage.site_archive_post_v2 s
            LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
        WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.ID = {postid} AND s.Date = '{date}';
    """
    print(insert_query)
    cursor.execute(insert_query)
    connection.commit()

def start_execution():
    siteid = 13
    labrador_api_url =  "https://nnf.dk/umbraco/api/news/newsfeed"
    print('starting data_insertion in pre_stage')

    # Fetch all data from the API
    truncate(13)
    fetch_all_labrador_data(labrador_api_url,siteid)
    date_correction()
  

def dev_etl():
    logging.info("Site13 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()