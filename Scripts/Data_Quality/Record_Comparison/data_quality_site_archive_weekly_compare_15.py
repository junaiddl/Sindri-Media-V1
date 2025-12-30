import logging
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
import requests, json
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

def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        connection.commit()
    except Exception as e:
        print(e)

def site11_API(cursor, connection, site_id):

    print(f"Start Date: {yesterday}")
    print(f"End Date: {today}")

    cms_api_url = f"https://sl.dk/umbraco/api/ContentDelivery/GetConentForSindri?fromdate={yesterday}T00:00:00&todate={today}T00:00:00"

    # cms_api_url = f"https://sl.dk/umbraco/api/ContentDelivery/GetConentForSindri?fromdate=2024-11-21&todate=2024-11-22"

    data_list = []
    print(len(data_list))
    response = requests.get(cms_api_url)
    print('response===', response.url)
    if response.status_code == 200:
        daily_data = response.json()
        print(len(response.json()))             
    else:
        logging.error("Failed to fetch articles: %s, %s", response.status_code, response.text)
        
    try:
        data_dict = daily_data
        for article in data_dict:
                
                postid = article.get("id", None)
                title = article.get('title', None)
                publisheddate = article.get('publishdate', None)
                print(publisheddate)
                print(yesterday_str)
                print(fetching_end_str)
                if(publisheddate < yesterday_str) or (publisheddate > fetching_end_str):
                    continue
                updatedate= article.get('updatedate', None)
                url = article.get('url', None)
                status ="published"
                categories = article.get('tagTargetGroup', None)
                tags = article.get('tagEvergreen', None)

                data_list.append((postid, title, publisheddate, url, categories, tags, status, updatedate, site_id))
        
        print("Data Fetched: ")
        for data in data_list:
              print(data)
        
        print("Total Legnth Of Data: " + str(len(data_list)))

        # Insert all fetched data into the pre_stage.labrador_data_temp table
        insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (ID,Title,Date,Link,Categories,Tags,Status,Modified,siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
            """
        try:
               cursor.executemany(insert_query, data_list)
               connection.commit()
               logging.info('Data insertion successful in pre_stage')
        except MySQLError as e:
                logging.error("Error inserting data into pre_stage MySQL database: %s", e)
                connection.rollback()
                raise

    except Exception as e:
                logging.error('data_insertion error: %s', e)
                raise

def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified,siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date), s.siteid 
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
                    WHERE d.ID IS NULL AND d.Date is null AND s.siteid = {siteid} AND s.ID = {postid} AND s.Date = '{date}';
    """
    print(insert_query)
    cursor.execute(insert_query)
    connection.commit()

def start_execution():
    connection = get_database_connection()
    cursor = connection.cursor()
    site_id = 15

    print("Truncating Data For Site 15 In Pre_Stage")
    truncate(15)

    # Fetch all data from the API for Site 15
    print("Started Insertion Into Pre_Stage")
    site11_API(cursor, connection,site_id)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()