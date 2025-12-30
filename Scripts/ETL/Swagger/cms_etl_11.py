
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


def site11_API(cursor, connection, batch_size, site_id, max_batch_id):
    start_date = datetime.today().date()
    start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
    end_date = start_date - timedelta(days=6)  
    current_date = start_date
    cms_api_url =  f"https://labrador.folkeskolen.dk/api/v1/article/?query=published:%5B{end_date}T00:00:00Z%20{start_date}T23:59:59Z%5D"
    cms_api_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiIsImtpZCI6InViRzBsSzRyUmkifQ.eyJqdGkiOiJZVE15TVdNMVpEQmtaVFIiLCJpYXQiOjE3Mzk1MjA0NDQsImF1ZCI6WyJjbXMiXSwic2NwIjpbImFwaS5yZWFkIl19.dZ6GlverLheozXwbMWla95D-fdmbXoIhBXu9mHGN4WM"
    
      # Set to store processed article IDs
  
    data_list =[]
    print(cms_api_url)
    print(len(data_list))
    print('given current_date', current_date)
    print('given end_date', end_date)
        
    headers = {"Authorization": f"Bearer {cms_api_token}"}
    for _ in range(batch_size):
            # params = {
            #     "published:gte": end_date,
            #     "published:lte": current_date,
               
            # }
            response = requests.get(cms_api_url, headers=headers)
            print('response===', response.url)
            if response.status_code == 200:
                daily_data = response.json()
                print(len(response.json()))
              
               
            else:
                logging.error("Failed to fetch articles: %s, %s", response.status_code, response.text)
            current_date -= timedelta(days=1)
            print("updated current date:" , current_date)
           

        
    try:
            data_dict = daily_data.get('result', [])
         
            
            for article in data_dict:
                postid = article.get("id", None)
                print(postid)
                
           
                title = article.get('title', None)
                published_Date = article.get('published', None)

                if published_Date and isinstance(published_Date, str):
                    result_datetime = parse(published_Date)
                    publishedDate =  result_datetime.strftime("%Y-%m-%d")

                else:
                    created = article.get('created', None)
                    if created and isinstance(created, str):
                        result_datetime = parse(created)
                        publishedDate = result_datetime.strftime("%Y-%m-%d")

                modified_Date = article.get('last_published_by', [None, None])[-1][1] if article.get('last_published_by') else None
                if modified_Date and isinstance(modified_Date, str):
                    result_datetime = parse(modified_Date)
                    modifiedDate = result_datetime.strftime("%Y-%m-%d")
               
                else:
                    created = article.get('created', None)
                    if created and isinstance(created, str):
                        result_datetime = parse(created)
                        modifiedDate = result_datetime.strftime("%Y-%m-%d")

                siteDomain = article.get('siteDomain', None)
                published_url = article.get('published_url', None)
                link = siteDomain + published_url if siteDomain and published_url else 'https://'
                is_published_hidden = article.get("is_publishedhidden")
                if is_published_hidden == "" or is_published_hidden == "0":
                    status = 'published'
                else:
                    status = 'hidden'
                sectionTag = article.get('section_tag', None)
                Tags = article.get('tags', None)
                data_list.append((postid, title, publishedDate, link, sectionTag, Tags, status, modifiedDate, max_batch_id, site_id))

            # insert all retrieved data into the pre_stage.labrador_data_temp table using the following query
            insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2(
            ID,
            Title,
            Date,
            Link,
            Categories,
            Tags,
            Status,
            Modified,
            batch_id,
            siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
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

def date_correction(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()

    # Correct the format of the Date column values
    update_query=f""" UPDATE pre_stage.site_archive_post_v2
    SET Date = DATE_FORMAT(Date, '%Y-%d-%m')  where Date > Current_Date and siteid ={siteid} """

    # Correct the format of the Modified Date column values
    update_query_2=f""" UPDATE pre_stage.site_archive_post_v2
    SET Modified = DATE_FORMAT(Modified, '%Y-%d-%m')  where Modified > Current_Date and siteid ={siteid}; """

    # Correct the grouping of the tag values in the Tags column
    update_query_3 = f"""UPDATE pre_stage.site_archive_post_v2
    SET tags = 
        CASE 
            WHEN tags LIKE '%BILLEDKUNST%' 
            OR tags LIKE '%BÃ˜RNEHAVEKLASSEN%' 
            OR tags LIKE 'DANSK'
            OR tags LIKE 'DANSK,%' 
            OR tags LIKE '%,DANSK' 
            OR tags LIKE '%, DANSK' 
            OR tags LIKE '%,DANSK,%' 
            OR tags LIKE ' DANSK,%' 
            OR tags LIKE '%, DANSK,%'  
            OR tags LIKE '%DSA%' 
            OR tags LIKE '%ENGELSK%' 
            OR tags LIKE '%HÃ…NDVÃ†RKOGDESIGN%' 
            OR tags LIKE '%IDRÃ†T%' 
            OR tags LIKE "%it,%" 
            OR tags LIKE ",%it,%" 
            OR tags LIKE "%,it%" 
            OR tags LIKE '%KULTURFAG%' 
            OR tags LIKE '%LÃ†RERSENIOR%' 
            OR tags LIKE '%LÃ†RERSTUDERENDE%' 
            OR tags LIKE '%MADKUNDSKAB%' 
            OR tags LIKE '%MATEMATIK%' 
            OR tags LIKE '%MUSIK%' 
            OR tags LIKE '%NATURFAG%' 
            OR tags LIKE '%PLC%' 
            OR tags LIKE '%PPR%' 
            OR tags LIKE '%PRAKTIK%' 
            OR tags LIKE '%SOSU%' 
            OR tags LIKE '%SPECIALPÃ†DAGOGIK%' 
            OR tags LIKE '%TYSKFRANSK%' 
            OR tags LIKE '%UU%'
            THEN 
                CASE 
                    WHEN tags LIKE '%Faglige netvÃ¦rk%'
                    THEN tags
                    ELSE CONCAT(tags, ',Faglige netvÃ¦rk')
                END
            ELSE tags
        END
    where siteid = {siteid};
    """ 
   
    try:
        cursor.execute(update_query)
        cursor.execute(update_query_2)
        cursor.execute(update_query_3)
        connection.commit()
        logging.info('Data correction successful in pre_stage')
    except MySQLError as e:
                logging.error("Error correction data into pre_stage MySQL database: %s", e)
                connection.rollback()

def insert_into_prod(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()

    # Get all data from pre_stage.site_archive_post_v2 for site 11 and send it to prod.site_archive_post_v2 for site 11
    # insert_query = f"""
    #     INSERT INTO prod.site_archive_post_v2 (ID, Title, Date, Link, categories, Tags, Status, Modified, batch_id,siteid)
    #         SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date),s.batch_id, s.siteid
    #                 FROM pre_stage.site_archive_post_v2 s
    #                 LEFT JOIN prod.site_archive_post_v2 d 
    #                     ON s.siteid = d.siteid and s.ID = d.ID 
    #                 WHERE
    #                 d.ID IS NULL AND d.Date is null and s.siteid = {siteid} and  s.Status = 'published' and length(s.Link)>20 and s.Date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    #         """

    siteid = 11
    start_date = datetime.today().strftime('%Y-%m-%d')

    insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, categories, userneeds, Tags, Status, Modified, batch_id, siteid, inserted_at)
        SELECT d.ID, d.Title, d.Date, d.Link, d.Categories, d.userneeds, d.Tags, d.Status, CAST(s.Modified AS DATE), d.batch_id, d.siteid, '{start_date}'
        FROM pre_stage.site_archive_post_v2 s
        LEFT JOIN prod.site_archive_post d
            ON s.siteid = d.siteid AND s.ID = d.ID 
        WHERE d.ID IS NOT NULL 
            AND d.Date IS NOT NULL 
            AND s.siteid = {siteid} 
            AND (s.Link != d.Link OR s.Date != d.Date) 
            AND s.Date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            AND s.Status = 'published'
        ON DUPLICATE KEY UPDATE
            Title = VALUES(Title),
            Date = VALUES(Date),
            Link = VALUES(Link),
            categories = VALUES(categories),
            userneeds = VALUES(userneeds),
            Tags = VALUES(Tags),
            Status = VALUES(Status),
            Modified = VALUES(Modified),
            batch_id = VALUES(batch_id),
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
                        WHERE s.siteid = {siteid} and s.Status = 'published' and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    """

    delete_old_records = f"""
        WITH IDS AS (
        SELECT d.ID
            FROM prod.site_archive_post d
            LEFT JOIN pre_stage.site_archive_post_v2 s
                ON s.siteid = d.siteid AND s.ID = d.ID
            WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} AND s.Status = 'published'
            AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        )
        DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
    """

    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, batch_id,siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date),s.batch_id, s.siteid
                FROM pre_stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID
                    WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.Status = 'published' AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
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
                logging.error("Error inserting data into pre_stage MySQL database: %s", e)
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
 
    max_batch_id = 0
    site_id = 11
    batch_size=1
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        # started truncate for siteid 11
        print("Started Truncate")
        truncate(11)
        truncate_data_quality(11)
        print('start data_insertion')
        # Get the CMS daily data from the API (Extract)
        site11_API(cursor, connection,batch_size,site_id, max_batch_id)
        print('start data correction')
        # Perform data correction (Transformation)
        date_correction(site_id)
        print('data correction done')
        # Insert the transformed data into the Production table (Load)
        # insert_into_prod(site_id)
        print("Data inserted in prod")
        send_data_quality(11, True)
    except:
        send_data_quality(11, False)


def dev_etl():
    logging.info("Site 11 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()