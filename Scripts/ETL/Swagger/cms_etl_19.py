import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse
import html
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)

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

yesterday = datetime.today().date() - timedelta(days=869)
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

def insert_into_stage(cursor, connection):
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

def fetch_all_labrador_data(api_url, siteid, category_mapping, tag_mapping, content_type = "post"):
    print(f"CONTENT TYPE IS: ----------- {content_type} ---------")
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

            # print(data)

            if not data:  # If the data is empty, break the loop
                break

            connection = get_database_connection()
            cursor = connection.cursor()

            # Extracting only the required fields
            extracted_data = []

            for article in data:
                print(article)
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
                

                # categories_list = ""
                # categories = article.get('categories', None)
                # if categories:
                #     comma = True
            
                #     for category in categories:
                #         category_val = category_mapping[category]         # OLD LOGIC FOR SINGLE ENDPOINT WITH CATEGORIES
                #         if comma:
                #             categories_list += category_val
                #             comma = False
                #         else:
                #             categories_list += ", " + category_val
                # else:
                #     categories_list = None


                if content_type == "page":
                    categories_list = ""
                else:
                    categories_list = ""
                    categories = article.get('categories', None)
                    if categories:
                        comma = True
                        for category in categories:
                            category_val = category_mapping.get(category, "")
                            if not category_val:
                                print(f"[WARNING] Category ID {category} not found in category_mapping.")
                            if comma:
                                categories_list += category_val
                                comma = False
                            else:
                                categories_list += ", " + category_val
                    else:
                        categories_list = None


                
                # tags_list = ""
                # tags = article.get('tags', None)
                # if tags:
                #     comma = True
            
                #     for tag in tags:
                #         tag_val = tag_mapping[tag]            # OLD LOGIC FOR SINGLE ENDPOINT WITH TAGS
                #         if comma:
                #             tags_list += tag_val
                #             comma = False
                #         else:
                #             tags_list += ", " + tag_val
                # else:
                #     tags_list = None

                if content_type == "page":
                    tags_list = "Pages"
                else:
                    tags_list = ""
                    tags = article.get('tags', None)
                    if tags:
                        comma = True
                        for tag in tags:
                            tag_val = tag_mapping.get(tag, "")
                            if not tag_val:
                                print(f"[WARNING] Tag ID {tag} not found in tag_mapping.")
                            if comma:
                                tags_list += tag_val
                                comma = False
                            else:
                                tags_list += ", " + tag_val
                    else:
                        tags_list = None


                print(postid, title, date, url, categories_list ,tags_list, status, modified, siteid)
                extracted_data.append((postid, title, date, url, categories_list ,tags_list, status, modified, siteid))
                
            insert_into_pre_stage(cursor, connection, extracted_data)

            if len(data) < 100:
                continue_fetching = False

            page += 1  # Increment the page number for the next iteration

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None


def insert_into_prod(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()
    start_date = datetime.today().strftime('%Y-%m-%d')
    
    try:

        insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (id, Title, Date, Link, Categories, Tags, Status, Modified, siteid, inserted_at)
            SELECT d.id, d.Title, d.Date, d.Link, d.Categories, d.Tags,  d.Status, d.Modified, d.siteid, '{start_date}'
                FROM stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.id 
                        WHERE d.id IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                            ON DUPLICATE KEY UPDATE
                                Title = VALUES(Title),
                                Date = VALUES(Date),
                                Link = VALUES(Link),
                                Categories = VALUES(Categories),
                                Tags = VALUES(Tags),
                                Status = VALUES(Status),
                                Modified = VALUES(Modified),
                                siteid = VALUES(siteid),
                                inserted_at = VALUES(inserted_at);
        """

        """AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""

        insert_link_updates_query = f"""
                INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                    SELECT s.id AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                        FROM stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post_historic d
                            ON s.siteid = d.siteid AND s.id = d.ID AND s.Link != d.Link
                                WHERE s.siteid = {siteid} and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        delete_old_records = f"""
            WITH IDS AS (
            SELECT d.ID
                FROM prod.site_archive_post d
                LEFT JOIN stage.site_archive_post_v2 s
                    ON s.siteid = d.siteid AND s.id = d.ID
                WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            )
            DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
        """

        insert_query = f"""
            INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, siteid)
                SELECT s.ID, s.Title, s.Date, s.Link, s.categories, s.Tags, s.Status, s.Modified, s.siteid
                    FROM stage.site_archive_post_v2 s
                    LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.id = d.ID 
                        WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        update_events = f"""
            UPDATE prod.events e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.event_name = lu.old_link
                AND lu.postid = e.postid
            SET e.event_name = lu.new_link
            WHERE e.siteid = lu.siteid AND e.event_name = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        update_pages = f"""
            UPDATE prod.pages e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.URL = lu.old_link
                AND lu.postid = e.postid
            SET e.URL = lu.new_link
            WHERE e.siteid = lu.siteid AND e.URL = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        update_traffic_channel = f"""
            UPDATE prod.traffic_channels e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.firsturl = lu.old_link
                AND lu.postid = e.postid
            SET e.firsturl = lu.new_link
            WHERE e.siteid = lu.siteid AND e.firsturl = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        # connection = get_database_connection()
        # cursor = connection.cursor()
        # cursor.execute(f"""
        #         INSERT INTO prod.site_archive_post_v2  (ID, Title, Date, Guid ,Date_gmt, Link, categories, Tags, categories_r, tags_r, Status, Modified, Modified_gmt, batch_id, siteid)
        #             SELECT s.ID, s.Title, s.Date, s.Guid , s.Date_gmt, s.Link, s.categories, s.Tags, s.categories_r, s.tags_r, s.Status, s.Modified, s.Modified_gmt, s.batch_id, s.siteid
        #                 FROM stage.site_archive_post_v2  s
        #                 LEFT JOIN prod.site_archive_post_v2 d ON s.ID = d.ID AND s.siteid = d.siteid
        #                     WHERE d.ID IS NULL and d.Date is null and s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        #     """)
        # connection.commit()

        cursor.execute(insert_into_historic)
        cursor.execute(insert_link_updates_query)
        cursor.execute(delete_old_records)
        cursor.execute(insert_query)
        cursor.execute(update_events)
        cursor.execute(update_pages)
        cursor.execute(update_traffic_channel)
        connection.commit()

        print('Data Inserted Into Prod')
    except Exception as e:
        print(e)

def send_data_quality(id, status):
    # Get current date and time
    current_date = (datetime.now()).strftime('%Y-%m-%d')
        
    try:
        # Connect to the MySQL database
        connection = get_database_connection()
        cursor = connection.cursor()

        # Insert error log into the data_quality_logs table
        insert_query = """
        INSERT INTO prod.posting_status (siteid, posting_date, cms_status)
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

def truncate_data_quality(site):
    connection = get_database_connection()
    cursor = connection.cursor()

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    print(current_date)

    sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND swagger_status IS NULL AND traffic_status IS NULL AND analytics_status IS NULL AND traffic_ingestion_status IS NULL and siteid = {site}"""
        
    cursor.execute(sql_query)
    connection.commit()  # Commit the transaction to save the changes
    print(cursor.rowcount, "record(s) deleted.")

    cursor.close()
    connection.close()

def start_execution():
    siteid = 19
    api_posts = "https://www.lif.dk/wp-json/wp/v2/posts"
    api_pages = "https://www.lif.dk/wp-json/wp/v2/pages"
    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        truncate_data_quality(siteid)
        print("Truncating Data For Site 19 In Pre_Stage")
        truncate(19)

        print('Starting data insertion in pre_stage')

        # Fetch all categories and tags from the API and insert into pre_stage
        category_mapping, tag_mapping = fetch_categories_and_tags()


        # Fetch all post data from the API and insert into pre_stage

        # ADDED content_type AND CALLING 2 TIMES, 1 FOR "post", 1 FOR "page"
        # Fetch posts 
        fetch_all_labrador_data(api_posts, siteid, category_mapping, tag_mapping, content_type="post") # ADDED content_type FOR 2 ENDPOINTS LOGIC
        # Fetch pages with 
        fetch_all_labrador_data(api_pages, siteid, category_mapping, tag_mapping, content_type="page") # ADDED content_type FOR 2 ENDPOINTS LOGIC
        
        insert_into_stage(connection=connection, cursor=cursor)
        
        # print('Starting data insertion in Prod')
        # # Insert all the fetched data into Production table
        # insert_into_prod(siteid)
        send_data_quality(siteid, True)
    except:
        send_data_quality(siteid, False)

def dev_etl():
    logging.info("Site 19 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()