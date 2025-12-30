import requests
import logging
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
from datetime import datetime, timedelta
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

    start_date = datetime.today().date()
    start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date = start_date - timedelta(days=1)  
    print(end_date)
    params = {
        'from':end_date,
        'to':start_date
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

def insert_into_prod():
    connection = get_database_connection()
    cursor = connection.cursor()
    # insert_query = """
    #   INSERT INTO prod.site_archive_post_v2 (ID, Title, Date, Link, categories, Tags, Status, Modified,siteid)
    #     SELECT s.ID, s.Title, s.date, s.link, s.categories, s.Tags, "publish", s.modified, s.siteid
    #         FROM pre_stage.site_archive_post_v2 s
    #         LEFT JOIN prod.site_archive_post_v2 d ON s.siteid = d.siteid and s.ID = d.ID 
    #         WHERE
    #         d.ID IS NULL AND d.Date is null and s.date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) and s.siteid = 13;
    #         """

    siteid = 13
    start_date = datetime.today().strftime('%Y-%m-%d')

    insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, categories, Tags, Status, Modified,siteid, inserted_at)
            SELECT d.ID, d.Title, d.date, d.link, d.categories, d.Tags, "publish", d.modified, d.siteid, '{start_date}'
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.ID 
                        WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
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
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified,siteid)
            SELECT s.ID, s.Title, s.date, s.link, s.categories, s.Tags, "publish", s.modified, s.siteid
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



def update_cat():
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query = """
     UPDATE pre_stage.site_archive_post_v2
        SET categories = 
        CASE categories
            WHEN 'news' THEN 'Nyhed'
            WHEN 'memberInfo' THEN 'Medlemsinfo'
            WHEN 'report' THEN 'Reportage'
            WHEN 'article' THEN 'Artikel'
            WHEN 'itCouldHappenToYouArticle' THEN 'Det ku'' ske for dig'
            WHEN 'editorial' THEN 'Jeg mener'
            WHEN 'videoArticle' THEN 'Videoartikel'
        ELSE categories
        END
        WHERE siteid = 13;
    """
    try:
        cursor.execute(insert_query)
        connection.commit()
        logging.info('Cat updated successful in prod')
    except MySQLError as e:
                logging.error("Error Cat updated successful in prod: %s", e)
                connection.rollback()

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
    siteid = 13
    labrador_api_url =  "https://nnf.dk/umbraco/api/news/newsfeed"
    print('starting data_insertion in pre_stage')
    try:
        # Fetch all data from the API
        truncate(13)
        truncate_data_quality(13)
        fetch_all_labrador_data(labrador_api_url,siteid)
        print("starting data correction")
        # Perform transformation on the data in the pre_stage table
        date_correction()
        update_cat()
        # Insert all the data into production
        print('starting data insertion in prod')
        # insert_into_prod()
        send_data_quality(13, True)
    except:
        send_data_quality(13, False)

  

def dev_etl():
    logging.info("Site13 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()
