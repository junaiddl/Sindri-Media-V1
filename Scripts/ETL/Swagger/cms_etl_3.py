import mysql.connector
import json
import html
import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)



# # Establish a connection to the MySQL database
def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host = os.getenv('DB_HOST'),
            port = os.getenv('DB_PORT'),
            user = os.getenv('DB_USER'),
            password = os.getenv('DB_PASSWORD')
        )
        return connection
#         cursor = connection.cursor()
        logging.info('successfull')
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)

# get tags values
def get_tag_values(list,url_t):
    try:
        values = ''
        for i in range(len(list)):
            url = url_t.format(str(list[i]))
            response = requests.get(url)
            tag_name = response.json()
            values += tag_name['name'] + '|'
        return values
    except Exception as e:
        logging.info(e)


# get catagories values
def get_categories_values(list,url_c):
    try:
        values = ''
        for i in range(len(list)):
            url = url_c.format(str(list[i]))
            response = requests.get(url)
            catagories = response.json()
            values += catagories['name'] + '|'
        return values
    except Exception as e:
        logging.info(e)


# get desired columns from json
def get_data(response):
    try:
        data = [{
            
            "ID": post["id"],
            "Title": post["title"]["rendered"],
            "Date": post["date"],
            "Date_gmt": post["date_gmt"],
            "guid": post["guid"]["rendered"],
            "Link": post["link"],
            "Categories": post["categories"],
            "Tags": post["tags"],
            "Status": post["status"],
            "Modified": post["modified"],
            "Modified_gmt": post["modified_gmt"]}
            for post in response.json()]
        return data

    except Exception as e:
        print(e)

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

def insert_into_prestage():
    columns_to_retrieve = ["id", "title", "link", "date","date_gmt","guid","tags", "categories","status","modified","modified_gmt"]
    connection = get_database_connection()
    cursor = connection.cursor()
    max_batch_id =  None
    # Target siteid for filtering rows
    siteid = 3  
    try:
        print('Siteid :',siteid)

        connection = get_database_connection()
        cursor = connection.cursor()
            
        # set the base URL of your WordPress site
        base_url = 'https://cphpost.dk/wp-json/wp/v2'

        # set the endpoint to get posts
        endpoint = '/posts'
        tag_url = 'https://cphpost.dk/wp-json/wp/v2/tags/{0}'
        cat_url = 'https://cphpost.dk/wp-json/wp/v2/categories/{0}'
        full_url = f'{base_url}{endpoint}?per_page=100'
        # set the date range to get posts from (last 24 hours)
        today = datetime.today().date()
        # print(f"Start Date: {today}")
        yesterday = today - timedelta(days=1)  
        # print(f"End Date: {yesterday}")

        # set the date format expected by the WordPress REST API
        today_str = today.strftime('%Y-%m-%dT%H:%M:%S')
        yesterday_str = yesterday.strftime('%Y-%m-%dT%H:%M:%S')
            
        # set the query parameters for the REST API request
        params = {
            "after": yesterday_str,
            "before": today_str,
            "per_page": 100, # set the number of posts to get per request
            "_fields": ",".join(columns_to_retrieve)
        }

        try:
            response = requests.get(f'{base_url}{endpoint}?per_page=100')
            print(f'{base_url}{endpoint}?per_page=100')
            exit()
            total_posts = int(response.headers['X-WP-Total'])
            # send the REST API request
            print('total_posts',total_posts)
            response = requests.get(base_url + endpoint, params=params)
            count = 0
            failed = 0
            # convert the list of dictionaries to a dataframe
            dataFrame = pd.DataFrame(get_data(response))
            if len(dataFrame.columns) != 0:
                dataFrame['batch_id'] = max_batch_id
                dataFrame['siteid'] = siteid
                # saving data into Staging
                cols = ",".join([str(i) for i in dataFrame.columns.tolist()])
                total = dataFrame.shape[0]

                for i, row in dataFrame.iterrows():
                    try:
                        row['Title'] = html.unescape(row['Title'])
                        row['tags_r'] = get_tag_values(row['Tags'], tag_url)
                        if pd.isnull(row['tags_r']) == True:
                            row['tags_r'] = ''
                        row['categories_r'] = get_categories_values(row['Categories'], cat_url)
                        if pd.isnull(row['categories_r']) == True:
                            row['categories_r'] = ''

                        row['Categories'] = row['categories_r']
                        row['Tags'] = row['tags_r']
                        
                        print(cols.replace('Categories', 'categories_r').replace('Tags', 'tags_r'))
                        print(row)

                        t = tuple(row)
                        sql = "INSERT INTO pre_stage.site_archive_post_v2 (" + cols.replace('Categories', 'categories_r').replace('Tags', 'tags_r') + ', Tags, Categories' +") VALUES (" + "%s," * (len(row) - 1) + "%s)"                            
                        a = cursor.execute(sql, tuple(row))
                        count = count+1
                    except Exception as e:
                        failed = failed+1
                        print(e)
                    
                    connection.commit()
                    desp = f"siteid  {siteid}  data"
        except Exception as e:
            print(e)
    except Exception as e:
                    print(e)

def insert_into_stage():

    connection = get_database_connection()
    cursor = connection.cursor()
    siteid = 3
    
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute(f"""
                INSERT INTO stage.site_archive_post_v2  (ID, Title, Date, Guid ,Date_gmt, Link, Categories_r, Tags_r, Status, Modified, Modified_gmt, batch_id,siteid)
                SELECT s.ID, s.Title, s.Date, s.Guid, s.Date_gmt, s.Link, s.Categories, s.Tags, s.Status, cast(s.Modified as date),s.Modified_gmt , s.batch_id, s.siteid
                    FROM pre_stage.site_archive_post_v2  s
                    LEFT JOIN stage.site_archive_post_v2 d ON s.ID = d.ID AND cast(s.Date as date) =  cast(d.Date as date)
                        WHERE d.ID IS NULL and d.Date is null and s.siteid = {siteid};"""
            )
        connection.commit()

        print('Data Inserted Into Stage')
    except Exception as e:
            print(e)

    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        siteid = 3
        cursor.execute(f"""
                UPDATE stage.site_archive_post_v2  d
                INNER JOIN pre_stage.site_archive_post_v2  s ON d.ID = s.ID AND d.Date = s.Date
                SET 
                    d.Title  = s.Title, 
                    d.Link = s.Link, 
                    d.categories = s.categories, 
                    d.Tags = s.Tags,
                    d.Guid = s.Guid,
                    d.Status = s.Status, 
                    d.Modified = s.Modified, 
                    d.Modified_gmt = s.Modified_gmt 
                WHERE 
                    d.Title  <> s.Title  OR
                    d.Link  <> s.Link OR
                    d.categories <> s.categories OR 
                    d.Tags <> s.Tags OR
                    d.Guid <> s.Guid OR
                    d.Status <> s.Status OR  
                    d.Modified  <> s.Modified OR
                    d.Modified_gmt <> s.Modified_gmt OR
                    d.batch_id <> s.batch_id OR
                    d.siteid <> s.siteid
                    and d.siteid = {siteid};""")
        connection.commit()
    except Exception as e:
        print(e)

    try:
        print("Updated Categories 2 for Siteid 3")
        cursor.execute(f"""
                        UPDATE stage.site_archive_post_v2
                        SET Categories =
                            CASE
                                WHEN Categories_r in ('News round up','Technology','Science','Sport','Politics') THEN 'News'
                                WHEN Categories_r in ('Explainer') THEN 'Explainer'
                                WHEN Categories_r in ('Business &amp; Education','Business & Education','Business','We Make Denmark Work')  THEN 'Business and Education'
                                WHEN Categories_r in ('Art','Art &amp; Culture','Art and Culture','Culture','Review')  THEN 'Art and Culture'
                                WHEN Categories_r in ('Things to do')  THEN 'Things to do'
                              ELSE SUBSTRING_INDEX(Categories_r, '|', 1)
                            END
                        WHERE
                             siteid = 3;""")
        connection.commit()
        print("Updated Categories 1 for Siteid 3")
        cursor.execute(f"""
                        UPDATE stage.site_archive_post_v2
                        SET Categories =
                            CASE
                                WHEN Categories_r LIKE '%News%' THEN 'News'
                                WHEN Categories_r LIKE '%Explainer%' THEN 'Explainer'
                                WHEN Categories_r LIKE '%Business and Education%' THEN 'Business and Education'
                                WHEN Categories_r LIKE '%Art and Culture%' THEN 'Art and Culture'
                                WHEN Categories_r LIKE '%Things to do%' THEN 'Things to do'
                              ELSE SUBSTRING_INDEX(Categories_r, '|', 1)
                            END
                        WHERE
                             siteid = 3;""")
        connection.commit()

        cursor.execute(f"""
                        UPDATE stage.site_archive_post_v2
                        SET tags = SUBSTRING_INDEX(Categories_r, '|', 1)
                        WHERE
                             siteid = 3;""")
        connection.commit()
    except Exception as e:
                print(e)

def insert_into_prod():
    connection = get_database_connection()
    cursor = connection.cursor()
    siteid = 3

    """ INSERT INTO prod.site_archive_post_v2  (ID, Title, Date, Guid ,Date_gmt, Link, categories, Tags, categories_r, tags_r, Status, Modified, Modified_gmt, batch_id, siteid)
                    SELECT s.ID, s.Title, s.Date, s.Guid , s.Date_gmt, s.Link, s.categories, s.Tags, s.categories_r, s.tags_r, s.Status, s.Modified, s.Modified_gmt, s.batch_id, s.siteid
                        FROM stage.site_archive_post_v2  s
                        LEFT JOIN prod.site_archive_post_v2 d ON s.ID = d.ID AND s.siteid = d.siteid
                            WHERE d.ID IS NULL and d.Date is null and s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    """
    
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        start_date = datetime.today().strftime('%Y-%m-%d')     

        insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Guid ,Date_gmt, Link, categories, Tags, categories_r, tags_r, Status, Modified, Modified_gmt, batch_id, siteid, inserted_at)
            SELECT d.ID, d.Title, d.Date, d.Guid , d.Date_gmt, d.Link, d.categories, d.Tags, d.categories_r, d.tags_r, d.Status, d.Modified, d.Modified_gmt, d.batch_id, d.siteid, '{start_date}'
                FROM stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.ID 
                        WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        insert_link_updates_query = f"""
                INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                    SELECT s.ID AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                        FROM stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post_historic d
                            ON s.siteid = d.siteid AND s.ID = d.ID AND s.Link != d.Link
                                WHERE s.siteid = {siteid} and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        delete_old_records = f"""
            WITH IDS AS (
            SELECT d.ID
                FROM prod.site_archive_post d
                LEFT JOIN stage.site_archive_post_v2 s
                    ON s.siteid = d.siteid AND s.ID = d.ID
                WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            )
            DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
        """

        insert_query = f"""
            INSERT INTO prod.site_archive_post (ID, Title, Date, Guid ,Date_gmt, Link, categories, Tags, categories_r, tags_r, Status, Modified, Modified_gmt, batch_id, siteid)
                SELECT s.ID, s.Title, s.Date, s.Guid , s.Date_gmt, s.Link, s.categories, s.Tags, s.categories_r, s.tags_r, s.Status, s.Modified, s.Modified_gmt, s.batch_id, s.siteid
                    FROM stage.site_archive_post_v2 s
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
        
        cursor.execute(insert_into_historic)
        cursor.execute(insert_link_updates_query)
        cursor.execute(delete_old_records)
        cursor.execute(insert_query)
        # cursor.execute(update_events)
        # cursor.execute(update_pages)
        # cursor.execute(update_traffic_channel)
        connection.commit()

        print('Data Inserted Into Prod')
    except Exception as e:
        print(e)

def truncate_data_quality(self, site):
    self.connection_db()
    cursor = self.cursor
    connection = self.connection

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    print(current_date)

    sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND cms_status IS NULL and siteid = {site}"""
        
    cursor.execute(sql_query)
    connection.commit()  # Commit the transaction to save the changes
    print(cursor.rowcount, "record(s) deleted.")

    cursor.close()
    connection.close()

def send_data_quality(self, id, status):
    # Get current date and time
    current_date = (datetime.now()).strftime('%Y-%m-%d')
        
    try:
        # Connect to the MySQL database
        self.connection_db()
        cursor = self.cursor
        connection = self.connection

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

def dev_etl():
    logging.info("Site Archive Post")
    # print("Started Truncate")
    truncate(3)
    # print("Started Insertion Into Pre_Stage")
    insert_into_prestage()
    # print("Started Insertion Into Stage")
    insert_into_stage()
    # print("Started Insertion Into Prod")
    insert_into_prod()