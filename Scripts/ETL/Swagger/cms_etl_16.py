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

def site11_API(cursor, connection, site_id,types):

    start_date = datetime.today().date()
    start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date = start_date - timedelta(days=1)  
    cms_api_url = f"https://blikroer.dk/api/endpoint?"

    page = 0
    continue_fetching = True

    while page<2 and continue_fetching:
        params = {
            'date_from': end_date,
            'date_to': start_date,
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

def insert_into_prod(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()
    siteid = 16
    start_date = datetime.today().strftime('%Y-%m-%d')

    # insert_query =f"""
    #     INSERT INTO prod.site_archive_post_v2 (ID, Title, Date, Link, Tags, Modified, siteid)
    #         SELECT s.ID, s.Title, s.Date, s.Link,s.Tags, cast(s.Modified as date), s.siteid 
    #             FROM pre_stage.site_archive_post_v2 s
    #             LEFT JOIN prod.site_archive_post_v2 d ON s.siteid = d.siteid and s.ID = d.ID 
    #                 WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    # """

    insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, Tags, Modified, siteid, inserted_at)
            SELECT d.ID, d.Title, d.Date, d.Link, d.Tags, cast(d.Modified as date), d.siteid, '{start_date}'
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.ID 
                        WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                        ON DUPLICATE KEY UPDATE
                            Title = VALUES(Title),
                            Date = VALUES(Date),
                            Link = VALUES(Link),
                            Tags = VALUES(Tags),
                            Modified = VALUES(Modified),
                            siteid = VALUES(siteid),
                            inserted_at = VALUES(inserted_at);
    """

    """AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""

    insert_link_updates_query = f"""
                INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                    SELECT s.ID AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                        FROM pre_stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post_historic d
                            ON s.siteid = d.siteid AND s.ID = d.ID AND s.Link != d.Link
                                WHERE s.siteid = {siteid} and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
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
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, Tags, Modified, siteid)
            SELECT s.ID, s.Title, s.Date, s.Link,s.Tags, cast(s.Modified as date), s.siteid 
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
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
    try:    
        connection = get_database_connection()
        cursor = connection.cursor()
        site_id = 16
        print("Truncating Data For Site 16 In Pre_Stage")
        truncate(16)
        truncate_data_quality(16)

        print('Started Data Insertion Into Pre_Stage')
        # Fetch data from two different end-points and insert into pre_stage
        type = ["nyhed","br_blog"]
        for types in type:
            print("API Type: " + str(types))
            site11_API(cursor, connection,site_id, types)

        # Insert the data into the Production table
        print("Started Data Ingestion Into Prod")
        insert_into_prod(site_id)
        send_data_quality(16, True)
        print("Data Ingested in Prod")
    except:
        send_data_quality(16, False)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()