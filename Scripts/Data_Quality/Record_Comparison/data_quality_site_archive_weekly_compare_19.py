import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse
import html
import os

def get_database_connection():
    try:
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

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')



def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        truncate_query2 = f"DELETE FROM stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query2)
        connection.commit()
        truncate_query3 = f"DELETE FROM prod.link_updates WHERE siteid = {siteid};"
        cursor.execute(truncate_query3)
    except Exception as e:
        print(e)

def insert_into_pre_stage(cursor, connection, data_list):
    print("Data To Be Inserted: ")
    for data in data_list:
        print(data)

    insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (id, Title, Date, Link, Categories, Tags, Status, Modified, siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info('Data insertion successful in pre_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into pre_stage MySQL database: %s", e)
        connection.rollback()
        raise

def fetch_categories_and_tags():
    categories_mapping = {}
    tags_mapping = {}
    continue_fetching = True
    page = 1
    while page <= 2 and continue_fetching:

        # Fetch categories
        categories_response = requests.get(f"https://www.lif.dk/wp-json/wp/v2/categories?per_page=100&page={page}") # MODIFIED
        categories_response.raise_for_status()
        categories = categories_response.json()
        print(len(categories))
        for category in categories:
            id = category["id"]
            value = category["name"]
            categories_mapping[id] = value
        # Create a dictionary for category ID to name mapping
        if len(categories) < 100:
                continue_fetching = False
        page += 1  # Increment the page number for the next iteration

    continue_fetching = True
    page = 1
    while page <= 2 and continue_fetching:
        # Fetch tags
        tags_response = requests.get(f"https://www.lif.dk/wp-json/wp/v2/tags?per_page=100&page={page}") # MODIFIED
        tags_response.raise_for_status()
        tags = tags_response.json()

        # Create a dictionary for tag ID to name mapping
        for tag in tags:
            id = tag["id"]
            value = tag["name"]
            tags_mapping[id] = value
        print(len(tags))
        if len(tags) < 100:
            continue_fetching = False
        else:
            page += 1  # Increment the page number for the next iteration
    return categories_mapping, tags_mapping

def fetch_all_labrador_data(api_url, siteid, category_mapping, tag_mapping):
    page = 1
    continue_fetching = True

    print(yesterday_str)
    print(today_str)

    while continue_fetching:
        params = {
            'page': page,
            'per_page': 100,
            'editorial_type' : 49882,
            'with' : 'contents,tags',
            'orderby' : 'date',
            'order' : 'desc',
            'after' : f'{yesterday_str}T00:00:00',
            'before' : f'{today_str}T23:59:59',
        }

        try:
            response = requests.get(api_url, params=params)
            # response.raise_for_status()
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
                title = title.get('rendered', None)
                date = article.get('date', None)
                if date and isinstance(date, str):
                    result_datetime = parse(date)
                    date = result_datetime.strftime("%Y-%m-%d")
                modified = article.get('modified', None)
                if modified and isinstance(modified, str):
                    result_datetime = parse(modified)
                    modified = result_datetime.strftime("%Y-%m-%d")
                url = article.get('link', None)
                if url:
                    parsed_url = urlparse(url)
                    url = urlunparse(parsed_url._replace(query=''))
                else:
                    url = None
                status = article.get('status', None)
                

                categories_list = ""
                categories = article.get('categories', None)
                if categories:
                    comma = True
            
                    for category in categories:
                        category_val = category_mapping[category]
                        if comma:
                            categories_list += category_val
                            comma = False
                        else:
                            categories_list += ", " + category_val
                else:
                    categories_list = None

                
                tags_list = ""
                tags = article.get('tags', None)
                if tags:
                    comma = True
            
                    for tag in tags:
                        tag_val = tag_mapping[tag]
                        if comma:
                            tags_list += tag_val
                            comma = False
                        else:
                            tags_list += ", " + tag_val
                else:
                    tags_list = None

                print(postid, title, date, url, categories ,tags, status, modified, siteid)
                extracted_data.append((postid, title, date, url, categories_list ,tags_list, status, modified, siteid))
                
            insert_into_pre_stage(cursor, connection, extracted_data)

            if len(data) < 100:
                continue_fetching = False

            page += 1  # Increment the page number for the next iteration

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

def insert_into_stage():
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query = '''
        INSERT INTO stage.site_archive_post_v2 (id, Title, Date, Link, Categories, Tags, Status, Modified, siteid)
        SELECT
            s.id,
            s.Title,
            s.Date,
            s.Link,
            s.Categories,
            s.Tags,
            s.Status,
            s.Modified,
            s.siteid
        FROM pre_stage.site_archive_post_v2 s
        WHERE s.siteid = 19 and s.Status = 'publish';'''
    
    url_update_query = '''
        UPDATE stage.site_archive_post_v2
        SET Link = LEFT(Link, LENGTH(Link) - 1)
        WHERE siteid = 19 and RIGHT(Link, 1) = '/';
        '''
    tags_update_query = '''
        UPDATE prod.site_archive_post
        SET tags = REPLACE(tags, '&amp;', '&')
        WHERE siteid = 19;
        '''
    try:
        cursor.execute(insert_query)
        connection.commit()
        logging.info('Data insertion successful into stage')

        cursor.execute(url_update_query)
        connection.commit()
        logging.info('url updated successfully in stage')

        cursor.execute(tags_update_query)
        connection.commit()
        logging.info('tags updated successfully in stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting or updating data into stage MySQL database: %s", e)
        connection.rollback()
        raise

def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.categories, s.Tags, s.Status, s.Modified, s.siteid
                FROM stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.id = d.ID 
                    WHERE d.ID IS NULL AND d.Date is null AND s.siteid = {siteid} AND s.ID = {postid} AND s.Date = '{date}';
    """
    print(insert_query)
    cursor.execute(insert_query)
    connection.commit()

def start_execution():
    siteid = 19
    api = "https://www.lif.dk/wp-json/wp/v2/posts"
    connection = get_database_connection()
    cursor = connection.cursor()

    print("Truncating Data For Site 19 In Pre_Stage")
    truncate(19)

    print('Starting data insertion in pre_stage')

    # Fetch all categories and tags from the API and insert into pre_stage
    category_mapping, tag_mapping = fetch_categories_and_tags()

    # Fetch all post data from the API and insert into pre_stage
    fetch_all_labrador_data(api, siteid, category_mapping, tag_mapping)
    insert_into_stage()

def dev_etl():
    logging.info("Site 19 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()