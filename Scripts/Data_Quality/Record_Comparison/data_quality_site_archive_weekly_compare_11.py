
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


def site11_API(cursor, connection, batch_size, site_id, max_batch_id):

    start_date = today
    end_date = yesterday
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
                    continue
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



def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};"
        cursor.execute(truncate_query1)
        connection.commit()
    except Exception as e:
        print(e)
        

def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
    INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, batch_id,siteid)
        SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date),s.batch_id, s.siteid
            FROM pre_stage.site_archive_post_v2 s
            LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID
                WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.Status = 'published' AND s.ID = {postid} AND s.Date = '{date}';
    """
    print(insert_query)
    cursor.execute(insert_query)
    connection.commit()
     

def start_execution():
    connection = get_database_connection()
    cursor = connection.cursor()
 
    max_batch_id = 0
    site_id = 11

    batch_size=1

    # started truncate for siteid 11
    print("Started Truncate")
    truncate(11)
    print('start data_insertion')
    # Get the CMS daily data from the API (Extract)
    site11_API(cursor, connection,batch_size,site_id, max_batch_id)
    print('start data correction')
    # Perform data correction (Transformation)
    date_correction(site_id)
    print('data correction done')

def dev_etl():
    logging.info("Site 11 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()