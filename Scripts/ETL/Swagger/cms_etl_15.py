
import logging
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error as MySQLError
from dateutil.parser import parse
import requests, json
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

def site11_API(cursor, connection, site_id):

    start_date = datetime.today().date()
    start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date = start_date - timedelta(days=100)

    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")

    print("params date:",end_date)
    cms_api_url = f" https://sl.dk/umbraco/api/ContentDelivery/GetConentForSindri?fromdate={end_date}&todate={start_date}"

    # cms_api_url = f"https://sl.dk/umbraco/api/ContentDelivery/GetConentForSindri?fromdate={end_date}&todate={start_date}"

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
                # publisheddate = article.get('publishdate', None)
                publisheddate = article.get('firstPublisedDate', None) # newly added, Mar 12
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

def insert_into_prod(siteid):

    start_date = datetime.today().strftime('%Y-%m-%d')     
    connection = get_database_connection()
    cursor = connection.cursor()

    insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, categories, Tags, userneeds, Status, Modified, siteid, inserted_at)
        SELECT d.ID, d.Title, d.Date, d.Link, d.Categories, d.Tags, d.userneeds, d.Status, CAST(d.Modified AS DATE), d.siteid, '{start_date}'
        FROM pre_stage.site_archive_post_v2 s
        LEFT JOIN prod.site_archive_post d
            ON s.siteid = d.siteid AND s.ID = d.ID 
        WHERE d.ID IS NOT NULL 
            AND d.Date IS NOT NULL 
            AND s.siteid = {siteid} 
            AND (s.Link != d.Link OR s.Date != d.Date) 
            AND s.Date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        ON DUPLICATE KEY UPDATE
            Title = VALUES(Title),
            Date = VALUES(Date),
            Link = VALUES(Link),
            categories = VALUES(categories),
            Tags = VALUES(Tags),
            userneeds = VALUES(userneeds),
            Status = VALUES(Status),
            Modified = VALUES(Modified),
            siteid = VALUES(siteid),
            inserted_at = VALUES(inserted_at);
    """
    
    """AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""

    # Insert old and new links into prod.link_updates with postid, date, and siteid for tracking
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
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date), s.siteid 
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

        logging.info('Data insertion successful into prod completed')
    except MySQLError as e:
                logging.error("Error inserting data into prod MySQL database: %s", e)
                connection.rollback()


def insert_into_site_group(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query =f"""
        INSERT INTO prod.site_archive_post_tags_groups_15   (id, siteid, Title, `Date`, Guid, Date_gmt, Link, Categories, Tags, Status, Modified, Modified_gmt, batch_id, tags_r, Categories_r, userneeds, excerpt, emner)
        SELECT id, siteid, Title, `Date`, Guid, Date_gmt, Link, Categories, Tags, Status, Modified, Modified_gmt, batch_id, tags_r, Categories_r, userneeds, excerpt,
        CASE 
            WHEN tags REGEXP "BÃ¸rn og unge|Anbringelse|Sikrede institutioner" THEN "BÃ¸rn og unge"
            WHEN tags REGEXP "Socialpolitik|Nyheder|Forbund og a-kasse|Coronavirus" THEN "Andet"
            WHEN tags REGEXP "Autisme|Udviklingshandicap|Botilbud|Magtanvendelse|Seksualitet|DomfÃ¦ldte" THEN "Handicap"
            WHEN tags REGEXP "Psykiatri|Misbrug|VÃ¦resteder|HjemlÃ¸se|Herberg og forsorgshjem|Marginaliserede" THEN "Psykiatri og udsathed"
            WHEN tags REGEXP "SocialpÃ¦dagogisk faglighed|SocialpÃ¦dagogisk praksis|Metoder og tilgange|Etik" THEN "SocialpÃ¦dagogisk faglighed"
            WHEN tags REGEXP "AnsÃ¦ttelsesvilkÃ¥r|Opsigelse|Overenskomst|Arbejdstid|LÃ¸n|Ferie|Barsel|Senior|Ligestilling|Job og karriere|Uddannelse|MeritpÃ¦dagoguddannelse|Kompetenceudvikling|Efteruddannelse|ArbejdsmiljÃ¸|Stress|Sygdom|Vold og trusler|Arbejdsskade|PTSD|A-kasse|LÃ¸nforsikring|Ledighed|EfterlÃ¸n|Dagpenge|ArbejdslÃ¸shed" THEN "Arbejdsliv og vilkÃ¥r"
        END as emner
        FROM 
            prod.site_archive_post_v2 where siteid = {siteid} and date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
      """
    try:
        cursor.execute(insert_query)
        connection.commit()
        logging.info('Data insertion successful into prod site group completed')
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
        site_id = 15
        print("Truncating Data For Site 15 In Pre_Stage")
        truncate(15)
        truncate_data_quality(15)

        # Fetch all data from the API for Site 15
        print("Started Insertion Into Pre_Stage")
        site11_API(cursor, connection,site_id)

        # Insert data into the Production table
        print("Data inserted in prod")
        # insert_into_prod(site_id)
        send_data_quality(15, True)
    except:
        send_data_quality(15, False)
        

    # insert_into_site_group(site_id) # This function is not used now in the current DBT version (can remove)

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()