import logging
from datetime import datetime, timedelta
import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse
import html
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

def site11_API(cursor, connection, site_id, types):

    cms_api_url = f"https://blikroer.dk/api/endpoint?"

    page = 0
    continue_fetching = True

    while page<2 and continue_fetching:
        params = {
            'date_from': yesterday,
            'date_to': today,
            'items_per_page': 100,   
            'type': types
        }
    
        response = requests.get(cms_api_url,params=params)
        print('response===', response.url)
        if response.status_code == 200:
            daily_data = response.json()         
        else:
            logging.error("Failed to fetch articles: %s, %s", response.status_code, response.text)
        data_list = []
        data_dict = daily_data
        # print(type(data_dict))
        for article in data_dict:
                postid = article.get("nid", None)
                title = article.get('title', None)
                if title:
                    title = html.unescape(title)
                    print(title)
                created_date_str = article.get('created', None)
                if created_date_str:
                    created_date = datetime.strptime(created_date_str.split(", ")[1].split(" - ")[0], "%d/%m/%Y")
                    publisheddate = created_date.strftime("%Y-%m-%d")

                    if(publisheddate < yesterday_str) or (publisheddate > fetching_end_str):
                        continue
                else:
                    publisheddate = None
                changed_date_str = article.get('changed', None)
                if changed_date_str:
                    changed_date = datetime.strptime(changed_date_str.split(", ")[1].split(" - ")[0], "%d/%m/%Y")
                    updatedate = changed_date.strftime("%Y-%m-%d")
                else:
                    updatedate = None
                url = article.get('view_node', None)
                if url:
                    parsed_url = urlparse(url)
                    url = urlunparse(parsed_url._replace(query=''))
                type = article.get('type', None)
                if type !="Nyhed":
                     tags = "Formanden"
                else:
                     tags = article.get('field_nyheds_kategorier', None)
             

                data_list.append((postid, title, publisheddate, url, tags,  updatedate, site_id))
        print("PAGE", page,":", len(data_list), data_list)
        if len(daily_data) < 100:
                continue_fetching = False

        page += 1        
        
        # Insert into pre_stage.labrador_data_temp table
        insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (ID,Title,Date,Link,Tags,Modified,siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s) 
            """
        
        print("Data To Be Inserted Into Pre_Stage")
        for data in data_list:
            print(data)

        try:
               cursor.executemany(insert_query, data_list)
               connection.commit()
               logging.info('Data insertion successful in pre_stage')
        except MySQLError as e:
                logging.error("Error inserting data into pre_stage MySQL database: %s", e)
                connection.rollback()
                raise
        
def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, Tags, Modified, siteid)
            SELECT s.ID, s.Title, s.Date, s.Link,s.Tags, cast(s.Modified as date), s.siteid 
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
    site_id = 16

    print("Truncating Data For Site 16 In Pre_Stage")
    truncate(16)

    print('Started Data Insertion Into Pre_Stage')
    # Fetch data from two different end-points and insert into pre_stage
    type = ["nyhed","br_blog"]
    for types in type:
        site11_API(cursor, connection,site_id, types)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()