import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
import html
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)

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

yesterday = datetime.today().date() - timedelta(days=1)
today = datetime.today().date()
yesterday_str = yesterday.strftime('%Y-%m-%d')

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
    # endpoint_categories = {'posts':'Artikel', 'latest_news':'Seneste nyt', 'guide':'Guide', 'debate':'Debat'}
    # endpoint = api_url.split("/")[-1]
    # category = endpoint_categories[endpoint]
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
                print(date)
                if date and isinstance(date, str):
                    result_datetime = parse(date)
                    date = result_datetime.strftime("%Y-%m-%d")
                modified = article.get('modified', None)
                if modified and isinstance(modified, str):
                    result_datetime = parse(modified)
                    modified = result_datetime.strftime("%Y-%m-%d")
                url = article.get('link', None)

                # if url == "https://fagbladet3f.dk/guide/helligdage-her-er-dine-fridage-i-2025/":
                #     print(api_url)
                #     print(url)
                #     print(date)

                cleaned_url = url.rstrip('/')

                # if url == "https://fagbladet3f.dk/guide/helligdage-her-er-dine-fridage-i-2025/":
                #     print(api_url)
                #     print(cleaned_url)
                #     print(date)

                # print(cleaned_url)
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


def insert_into_prod():
    connection = get_database_connection()
    cursor = connection.cursor()
    siteid = 11
    start_date = datetime.today().strftime('%Y-%m-%d')


    # insert_query = """
    #     INSERT INTO prod.site_archive_post_v2 (ID, Title, Date, Link, categories, Tags,status, Modified,siteid)
    #         SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.status,cast(s.Modified as date), s.siteid
    #                 FROM pre_stage.site_archive_post_v2 s
    #                 LEFT JOIN prod.site_archive_post_v2 d ON s.siteid = d.siteid and s.ID = d.ID 
    #             WHERE
    #                 d.ID IS NULL AND d.Date is null and s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and s.siteid = 14;
    #         """

    insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, categories, Tags,status, Modified,siteid, inserted_at)
            SELECT d.ID, d.Title, d.Date, d.Link, d.Categories, d.Tags, d.status, cast(d.Modified as date), d.siteid, '{start_date}'
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.ID 
                        WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);;
    """

    """AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""

    insert_link_updates_query = f"""
                INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                    SELECT s.ID AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                        FROM pre_stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post_historic d
                            ON s.siteid = d.siteid AND s.ID = d.ID AND s.Link != d.Link
                                WHERE s.siteid = {siteid} and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);;
    """

    delete_old_records = f"""
            WITH IDS AS (
            SELECT d.ID
                FROM prod.site_archive_post d
                LEFT JOIN pre_stage.site_archive_post_v2 s
                    ON s.siteid = d.siteid AND s.ID = d.ID
                WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            )
            DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
    """

    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags,status, Modified,siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.status,cast(s.Modified as date), s.siteid
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
            WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);;
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

    try:
        cursor.execute(insert_into_historic)
        cursor.execute(insert_link_updates_query)
        cursor.execute(delete_old_records)
        cursor.execute(insert_query)
        cursor.execute(update_events)
        cursor.execute(update_pages)
        cursor.execute(update_traffic_channel)
        connection.commit()
        logging.info('Data insertion successful in prod')
    except MySQLError as e:
                logging.error("Error inserting data into pre_stage MySQL database: %s", e)
                connection.rollback()

# posts=Artikel | news_overview=Nyhedsoverblik | guide=Guide | digidoc=Digidoc | debate=Debat

def update_cat(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query = f"""
     UPDATE pre_stage.site_archive_post_v2
        SET Categories = 
        CASE Categories
            WHEN 'post' THEN 'Artikel'
            WHEN 'latest_news' THEN 'Seneste nyt'
            WHEN 'guide' THEN 'Guide'
            WHEN 'debate' THEN 'Debat'
            WHEN 'feature' THEN 'Feature'
        ELSE Categories
        END
    WHERE siteid = {siteid};
    """
    try:
        cursor.execute(insert_query)
        connection.commit()
    except MySQLError as e:
        connection.rollback()
     


def update_tags(siteid):
   response = requests.get("https://folkeskolen.dk/wp-json/wp/v2/subject")
   response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
   tags_list = response.json()
   for tag in tags_list:
        tag_id = tag['id']
        tag_name = tag['name']
        print(tag_name,tag_id)
        connection = get_database_connection()
        cursor = connection.cursor()
        try:
            update_query= f"update pre_stage.site_archive_post_v2 set Tags = '{tag_name}' where Tags= '{tag_id}' and siteid = {siteid}"
            cursor.execute(update_query)
            connection.commit()
            connection.close()
        except mysql.connector.Error as error:
            print(error)
              

def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        truncate_query2 = f"DELETE FROM stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query2)
        truncate_query3 = f"DELETE FROM prod.link_updates WHERE siteid = {siteid};"
        cursor.execute(truncate_query3)
        connection.commit()
    except Exception as e:
        print(e)

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

def start_execution():
    siteid = 11
    labrador_api_url = [ 
"http://folkeskolen.dk/wp-json/wp/v2/posts",
"http://folkeskolen.dk/wp-json/wp/v2/debate",
"http://folkeskolen.dk/wp-json/wp/v2/guide",
"https://folkeskolen.dk/wp-json/wp/v2/latest_news"]
    try:
        print(f"Truncating Data For Site {siteid} In Pre_Stage")
        truncate(siteid)
        truncate_data_quality(siteid)

        # for each API Url, fetch data and insert it into pre_stage
        print("Starting Data Ingestion Into Pre_Stage")
        for api in labrador_api_url:
            print("API: ", api)
            fetch_all_labrador_data(api, siteid)

        # update the tag and categories values for site 11
        print("Updating Tags")
        update_tags(siteid)
        print("Updating Categories")
        update_cat(siteid)

        # insert cleaned data into prod table
        # print("Started Data Ingestion Into Prod")
        # insert_into_prod()
        # send_data_quality(siteid, True)
        # print("Data Ingestion Into Prod Completed")
    except:
        send_data_quality(siteid, False)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()