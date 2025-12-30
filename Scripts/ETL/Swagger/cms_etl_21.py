import logging
import mysql.connector
import mysql
import requests
import json
from urllib.parse import urljoin
from datetime import datetime, timedelta
import os
import sys
config_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(1, config_path)
from config_file import host, port, user, password


def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
          host = host,
          port = port,
          user = user,
          password = password
        )

        return connection
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)


def fetch_articles():
    # yesterday = datetime.today().date() - timedelta(days=1)
    today = datetime.today().date()
    yesterday = datetime.today().date() - timedelta(days=1)

    # Convert to strings for later use
    # from_date = yesterday.strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    from_date = yesterday.strftime("%Y-%m-%d")

    url = "https://bogodtbladet.dk/umbraco/api/articlelist/feed"
    params = {
        "publishedFrom": from_date,
        "publishedTo": to_date,
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def truncate():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = 21;"
        cursor.execute(truncate_query1)
        connection.commit()
        print("Truncated pre_stage.site_archive_post_v2 successfully.")
    except Exception as e:
        print(e)

def insert_into_pre_stage(cursor, connection, data_list):
    print("Starting To Insert Data Into Pre_Stage")
    for data in data_list:
        print(data)

    insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (id,Title,Date,Modified,Link,Categories,Tags,siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info('Data insertion successful in pre_stage')
        url_update_query = '''
        UPDATE pre_stage.site_archive_post_v2
        SET Link = LEFT(Link, LENGTH(Link) - 1)
        WHERE siteid = 21 and RIGHT(Link, 1) = '/';
        '''
        cursor.execute(url_update_query)
        connection.commit()
        logging.info('URL update successful in  pre_stage')


    except mysql.connector.Error as e:
        logging.error("Error inserting data into pre_stage MySQL database: %s", e)
        connection.rollback()
        raise




def insert_article_to_db():
    connection = get_database_connection()
    cursor = connection.cursor()
    data = fetch_articles()
    extracted_data = []
    for item in data:
            id= item.get("id")
            title= item.get("title")
            date= item.get("date")
            updatedDate= item.get("updatedDate")
            url= item.get("url")
            category= item.get("category")
            pageType= item.get("pageType")
            siteid =21

            extracted_data.append((id, title, date, updatedDate, url, category, pageType, siteid))

    insert_into_pre_stage(cursor, connection, extracted_data)
    connection.close()

        
    


def insert_into_prod_stage():
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO prod.site_archive_post(id, siteid, Title, Date, Modified, Link, Categories, Tags)
    SELECT
        s.id,
        s.siteid,
        s.Title,
        s.Date,
        s.Modified,
        s.Link,
        s.Categories,
        s.Tags
    FROM pre_stage.site_archive_post_v2 s
    LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid AND s.id = d.ID
    WHERE d.ID IS NULL AND d.Date IS NULL
    and s.siteid = 21;"""
    try:
        cursor.execute(insert_query)
        connection.commit()
        logging.info('Data insertion successful in prod_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into prod_stage MySQL database: %s", e)
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


def start_execution():
    truncate()
    insert_article_to_db()
    insert_into_prod_stage()

def dev_etl():
    logging.info("Site 21 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()


  


