import mysql.connector
import json
import requests
import logging
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
        print('Database Connected...')
        return connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)

def truncate_table():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = "DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = 4;"
        cursor.execute(truncate_query1)
        truncate_query2 = "DELETE FROM stage.site_archive_post_v2 WHERE siteid = 4;"
        cursor.execute(truncate_query2)
        truncate_query3 = f"DELETE FROM prod.link_updates WHERE siteid = 4;"
        cursor.execute(truncate_query3)
        connection.commit()
        print("pre_stage.site_archive_post_v2 & stage.site_archive_post_v2 truncated")
    except Exception as e:
        print("An error occurred during table truncating", str(e))

def insert_into_prestage():
    connection = get_database_connection()
    cursor = connection.cursor()

    max_batch_id = None
    site_id = 4

    try:        
        # Get yesterday's date
        start_date = datetime.today().date()
        start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
        end_date = start_date - timedelta(days=6)  
        # print(yesterday)
        # Format the date in the desired format (YYYY-MM-DD)

        url = f"https://piopio.dk/artikler-mellem?created[min]={end_date}+00%3A00%3A00&created[max]={start_date}+23%3A59%3A59"
        # url = f"https://piopio.dk/artikler-mellem?created[min]=2025-03-28+00%3A00%3A00&created[max]=2025-03-29+23%3A59%3A59"
        # url = f"https://piopio.dk/artikler-mellem?created%5Bmin%5D={end_date}+00%3A00%3A00&changed%5Bmax%5D={start_date}+23%3A59%3A59"
        # url = f"https://piopio.dk/artikler-mellem?changed%5Bmin%5D=2024-10-15+00%3A00%3A00&changed%5Bmax%5D=2024-10-09+23%3A59%3A59"
        response = requests.get(url)
        print(url)
        data = response.json()
        print('start')
        if response.status_code == 200:
            # select_query = "select * from prod.cms_user_configuration where is_active = 1 and cms_type = 'Drupal'"
            # cursor.execute(select_query)
            # cred = cursor.fetchall()
            # a_url = cred[0][2]
            may = []
            if data:
                try:
                    for item in data:
                        Article_id = item['article_id']
                        Title = item['title']
                        Pub_date = item['pub_date']
                        Last_updated = item['last_updated']
                        Short_url = item['short_url']
                        Url = item['url']
                        Category_id = item['category_id']
                        Category = item['category']
                        Tags_id = item['tags_id']
                        Tags = item['tags']
                        Status = item['status']

                    #  Convert last_updated to the desired format
                        last_updated_dt = datetime.strptime(Last_updated, "%d.%m.%Y - %H:%M")
                        Last_updated_formatted = last_updated_dt.strftime("%Y-%m-%d %H:%M:%S")

                        date_only = last_updated_dt.strftime("%Y-%m-%d")


                        Pub_date_dt = datetime.strptime(Pub_date, "%d.%m.%Y - %H:%M")
                        Pub_date_formatted = Pub_date_dt.strftime("%Y-%m-%d %H:%M:%S")
                        if Status == "Nej":
                            active = 0
                        else:
                            active = 1

                        may.append(
                            [
                                int(Article_id), Title, Pub_date_dt, Pub_date_formatted, Short_url, Url, Category,
                                Tags, Status, site_id, max_batch_id, last_updated_dt, Last_updated_formatted, active
                            ]
                        )
                    # Insert data into pre_stage.site_archive_post_temp
                    print(may)
                    query = """
                    INSERT INTO pre_stage.site_archive_post_v2 (
                        ID, Title, Date, Date_gmt, Guid, Link, Categories, Tags, Status, siteid, Batch_id, Modified, Modified_gmt, active
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(query, may)
                    connection.commit()
                except:
                    print("List Index Out Of Range! No Data For Today!")
    except mysql.connector.Error as error:
        print("Error inserting ETL log:", error)

def insert_into_stage():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO stage.site_archive_post_v2 (
                ID, Title, Date, Guid, Date_gmt, Link, categories, Tags, Status, Modified, Modified_gmt, batch_id, siteid, active)
                SELECT
                    s.ID, s.Title, s.Date, s.Guid, s.Date_gmt, s.Link, s.Categories, s.Tags, s.Status,
                    cast(s.Modified as date), cast(s.Modified_gmt as date), s.batch_id, s.siteid, s.active
                FROM
                    pre_stage.site_archive_post_v2 s
                    LEFT JOIN stage.site_archive_post_v2 d ON s.ID = d.ID AND cast(s.Date as date) = cast(d.Date as date)
                WHERE s.siteid = 4 and d.ID IS NULL AND d.Date is null and s.active = 1;
            """)
        connection.commit()
        print('inserted complete in stage in site_archive_post table ')
        cursor = connection.cursor()
        connection.commit()
        connection.close()
    except Exception as e:
        print(e)

    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        print('Update start')
        cursor.execute("""
            UPDATE stage.site_archive_post_v2 d
            INNER JOIN pre_stage.site_archive_post_v2 s ON d.ID = s.ID AND d.Date = s.Date  AND s.siteid = d.siteid
            SET
                d.Title = s.Title,
                d.Link = s.Link,
                d.categories = s.categories,
                d.Tags = s.Tags,
                d.Guid = s.Guid,
                d.Status = s.Status,
                d.Modified = s.Modified,
                d.Modified_gmt = s.Modified_gmt
            WHERE
                    
                d.Title <> s.Title OR
                d.Link <> s.Link OR
                d.categories <> s.categories OR
                d.Tags <> s.Tags OR
                d.Guid <> s.Guid OR
                d.Status <> s.Status OR
                d.Modified <> s.Modified OR
                d.Modified_gmt <> s.Modified_gmt OR
                d.siteid <> s.siteid and
                s.active = 1;
            """)
        connection.commit()
    except Exception as e:
        print(e)

def insert_into_prod():

    connection = get_database_connection()
    cursor = connection.cursor()

    # query = """
    #             INSERT INTO prod.site_archive_post_v2 (
    #                 ID, Title, Date, Guid, Date_gmt, Link, Categories, Tags, Status, Modified, Modified_gmt, batch_id, siteid, active
    #             )
    #             SELECT
    #                 s.ID, s.Title, s.Date, s.Guid, s.Date_gmt, s.Link, s.Categories, s.Tags, s.Status,
    #                 cast(s.Modified as date), cast(s.Modified_gmt as date), s.batch_id, s.siteid, s.active
    #             FROM
    #                 stage.site_archive_post_v2 s
    #                 LEFT JOIN prod.site_archive_post_v2 d ON s.ID = d.ID AND cast(s.Date as date) = cast(d.Date as date)
    #             WHERE s.siteid = 4 and d.ID IS NULL AND d.Date is null and s.active = 1 AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
    #         """
    try:

        start_date = datetime.today().strftime('%Y-%m-%d')    
        siteid = 4 

        insert_into_historic = f"""
            INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Guid, Date_gmt, Link, Categories, userneeds, Tags, Status, Modified, Modified_gmt, batch_id, siteid, inserted_at)
                SELECT d.ID, d.Title, d.Date, d.Guid, d.Date_gmt, d.Link, d.Categories, d.userneeds, d.Tags, d.Status,
                        cast(d.Modified as date), cast(d.Modified_gmt as date), d.batch_id, d.siteid, '{start_date}'
                    FROM stage.site_archive_post_v2 s
                    LEFT JOIN prod.site_archive_post d
                        ON s.siteid = d.siteid AND s.ID = d.ID 
                            WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                            AND (s.Link != d.Link OR s.Date != d.Date)
                                ON DUPLICATE KEY UPDATE
                                    Title = VALUES(Title),
                                    Date = VALUES(Date),
                                    Guid = VALUES(Guid),
                                    Date_gmt = VALUES(Date_gmt),
                                    Link = VALUES(Link),
                                    Categories = VALUES(Categories),
                                    userneeds = VALUES(userneeds),
                                    Tags = VALUES(Tags),
                                    Status = VALUES(Status),
                                    Modified = VALUES(Modified),
                                    Modified_gmt = VALUES(Modified_gmt),
                                    batch_id = VALUES(batch_id),
                                    siteid = VALUES(siteid),
                                    inserted_at = VALUES(inserted_at);
        """

        insert_link_updates_query = f"""
                    INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                        SELECT s.ID AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                            FROM stage.site_archive_post_v2 s
                            LEFT JOIN prod.site_archive_post_historic d
                                ON s.siteid = d.siteid AND s.ID = d.ID AND s.Link != d.Link
                                    WHERE s.siteid = {siteid} and d.LINK IS NOT NULL;
        """

        delete_old_records = f"""
                WITH IDS AS (
                SELECT d.ID
                    FROM prod.site_archive_post d
                    LEFT JOIN stage.site_archive_post_v2 s
                        ON s.siteid = d.siteid AND s.ID = d.ID
                    WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                    AND (d.Link != s.Link OR d.Date != s.Date)
                )
                DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
        """

        insert_query = f"""
                INSERT INTO prod.site_archive_post (ID, Title, Date, Guid, Date_gmt, Link, Categories, Tags, Status, Modified, Modified_gmt, batch_id, siteid)
                    SELECT s.ID, s.Title, s.Date, s.Guid, s.Date_gmt, s.Link, s.Categories, s.Tags, s.Status,
                        cast(s.Modified as date), cast(s.Modified_gmt as date), s.batch_id, s.siteid
                        FROM stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.ID = d.ID 
                            WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid};
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
        cursor.execute(update_events)
        cursor.execute(update_pages)
        cursor.execute(update_traffic_channel)
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
    try:
        truncate_table()
        truncate_data_quality(4)
        insert_into_prestage()
        insert_into_stage()
        # insert_into_prod()
        send_data_quality(4,True)
    except:
        send_data_quality(4,False)

# Call the main function to start your program
# create a object of class
def dev_etl():
    logging.info("Site Archive Post")
    start_execution()

dev_etl()