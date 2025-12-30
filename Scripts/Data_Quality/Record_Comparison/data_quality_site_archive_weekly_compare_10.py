import mysql.connector
import json
import html
import logging
import requests
from datetime import datetime, timedelta
import pandas as pd
import os

yesterday = datetime.today().date() - timedelta(days=7)
yesterday_str = yesterday.strftime('%Y-%m-%d')

fetching_end = datetime.today().date() - timedelta(days=1)
fetching_end_str = fetching_end.strftime('%Y-%m-%d')

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')

# # Establish a connection to the MySQL database
def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
             host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            port = int(os.getenv("DB_PORT")),
        )
        return connection
#       cursor = connection.cursor()
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
        connection.commit()
    except Exception as e:
        print(e)

def insert_into_prestage():
    columns_to_retrieve = ["id", "title", "link", "date","date_gmt","guid","categories","tags","status","modified","modified_gmt"]
    connection = get_database_connection()
    cursor = connection.cursor()
    max_batch_id =  None
    # Target siteid for filtering rows
    siteid = 10  
    try:
        print('Siteid :',siteid)

        connection = get_database_connection()
        cursor = connection.cursor()
            
        # set the base URL of your WordPress site
        base_url = 'https://corpes.dk/wp-json/wp/v2'

        # set the endpoint to get posts
        endpoint = '/posts'
        tag_url = 'https://corpes.dk/wp-json/wp/v2/tags/{0}'
        cat_url = 'https://corpes.dk/wp-json/wp/v2/categories/{0}'
        full_url = f'{base_url}{endpoint}?per_page=100'

        # set the date range to get posts from (last 24 hours)
        today = datetime.today().date()
        # print(f"Start Date: {today}")
        yesterday = today - timedelta(days=5)  
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
        # https://corpes.dk/wp-json/wp/v2/posts?per_page=100
        try:
            response = requests.get(f'{base_url}{endpoint}?per_page=100')
            
            total_posts = int(response.headers['X-WP-Total'])
            # send the REST API request
            print('total_posts',total_posts)
            response = requests.get(base_url + endpoint, params=params)
            print(get_data(response))
            count = 0
            failed = 0
            # convert the list of dictionaries to a dataframe
            dataFrame = pd.DataFrame(get_data(response))
            print(f"data in pandas dataframe:{dataFrame}")
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
                        print(tuple(row))

                        t = tuple(row)
                        sql = "INSERT INTO pre_stage.site_archive_post_v2 (" + cols.replace('Categories', 'categories_r').replace('Tags', 'tags_r') + ', Categories, Tags' +") VALUES (" + "%s," * (len(row) - 1) + "%s)"                            
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
    siteid = 10
    
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
        siteid = 10
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
        print("Updated Categories for Siteid 10")
        cursor.execute(f"""
                        UPDATE stage.site_archive_post_v2
                        SET Categories =
                            CASE
                                WHEN Categories_r LIKE '%Fodsundhed og samfund%' THEN 'Fodsundhed og samfund'
                                WHEN Categories_r LIKE '%Forebyggelse%' THEN 'Forebyggelse'
                                WHEN Categories_r LIKE '%Forskning og viden%' THEN 'Forskning og viden'
                                WHEN Categories_r LIKE '%Praksis%' THEN 'Praksis'
                                WHEN Categories_r LIKE '%Ukategoriseret%' THEN 'Ukategoriseret'
                              ELSE SUBSTRING_INDEX(Categories_r, '|', 1)
                            END
                        WHERE
                             siteid = 10;""")
        
        print("Updated Tags for Siteid 10")
        cursor.execute(f"""UPDATE stage.site_archive_post_v2
                       SET Tags =  
                        CASE
                            WHEN Tags_r LIKE '%Giv mig en fordel%' THEN 'Giv mig en fordel'
                            WHEN Tags_r LIKE '%Hjælp mig med at forstå%' THEN 'Hjælp mig med at forstå'
                            WHEN Tags_r LIKE '%Inspirer mig%' THEN 'Inspirer mig'
                            WHEN Tags_r LIKE '%Best Practice%' THEN 'Best Practice'
                            WHEN Tags_r LIKE '%Kort og godt%' THEN 'Kort og godt'
                            WHEN Tags_r LIKE '%Long read%' THEN 'Long read'
                            WHEN Tags_r LIKE '%Q&A%' THEN 'Q&A'
                            WHEN Tags_r LIKE '%Viden og forskning%' THEN 'Viden og forskning'
                        ELSE SUBSTRING_INDEX(Tags_r, '|', 1)
                        END
                        WHERE siteid = 10;""")
    except Exception as e:
                print(e)

def insert_into_prod(siteid, postid, link, date, cursor, connection):
    insert_query = f"""
        INSERT INTO prod.site_archive_post (ID, Title, Date, Guid ,Date_gmt, Link, categories, Tags, categories_r, tags_r, Status, Modified, Modified_gmt, batch_id, siteid)
            SELECT s.ID, s.Title, s.Date, s.Guid , s.Date_gmt, s.Link, s.categories, s.Tags, s.categories_r, s.tags_r, s.Status, s.Modified, s.Modified_gmt, s.batch_id, s.siteid
                FROM stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
                    WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.ID = {postid} AND s.Date = '{date}';
    """
    cursor.execute(insert_query)
    connection.commit()
    print(insert_query)

def dev_etl():
    logging.info("Site Archive Post")
    print("Started Truncate")
    truncate(10)
    print("Started Insertion Into Pre_Stage")
    insert_into_prestage()
    insert_into_stage()

dev_etl()