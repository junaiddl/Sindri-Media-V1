import mysql.connector
import time
from datetime import datetime, timedelta

start_date = datetime.today().date()
start_date = datetime.strptime(str(start_date), "%Y-%m-%d").date()
end_date = start_date - timedelta(days=1)  


def insert_data_into_stage_15_16_17(cursor, connection,id):

        print('insert_stage')

        domain_query = f"""
        SELECT domain FROM prod.domain_siteid WHERE siteid = {id};
        """
        cursor.execute(domain_query)
        domain_fetch_result = cursor.fetchone()

        if domain_fetch_result is not None:
            # Extract the string value from the tuple
            domain_value = domain_fetch_result[0]
            print(domain_value)
        else:
            print("No domain found for the specified site ID")

        connection.commit()
        print(domain_fetch_result)

        update_events_urls = f"""
        UPDATE pre_stage.events
        SET event_name = REPLACE(event_name, ':///', '{domain_value}')
        WHERE siteid = {id} AND event_name LIKE '://%';
        """

        remove_param_page= f"""
        UPDATE pre_stage.pages a
        SET a.URL = 
            CASE
                WHEN a.URL REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.URL, '?', 1)
                ELSE a.URL
            END
        WHERE a.SITEID = {id};

        """

        remove_param_events = f"""
        UPDATE pre_stage.events a
        SET a.URL = 
            CASE
                WHEN a.event_name REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.event_name, '?', 1)
                ELSE a.event_name
            END
        WHERE a.SITEID = {id};

        """

        print(update_events_urls)
        cursor.execute(update_events_urls)
        connection.commit()

        cursor.execute(remove_param_page)
        cursor.execute(remove_param_events)
        connection.commit()

        try:
            query1 = f"""        
                INSERT INTO stage.pages (postid, siteid, date, URL, unique_pageviews, batch_id)
                SELECT b.ID AS postid, a.siteid, a.date, a.URL, a.unique_pageviews, a.batch_id
                FROM pre_stage.pages a
                LEFT JOIN prod.site_archive_post b ON a.URL = b.Link
                WHERE  a.siteid = {id} AND a.date = '{end_date}';
            """
            cursor.execute(query1)
            print('1')
            connection.commit()
        except mysql.connector.Error as error:
            print(f"Error: {error}")

        try:
            query_1 = f"""        
            insert into stage.pages_temp_s (siteid,date,postid,URL,batch_id,unique_pageviews)
            select siteid,date,postid,MAX(URL),batch_id,sum(unique_pageviews) as  unique_pageviews from stage.pages
            where siteid ={id}
            group by siteid, date, postid, batch_id;
            """
            cursor.execute(query_1)
            connection.commit()
            print('2')
        except mysql.connector.Error as error:
            print(f"Error: {error}")
        
        try:
            query2 = f"""
                INSERT INTO stage.events  (postid, Date, Event_Category, Event_Action, event_name, hits, batch_id,siteid )
                SELECT b.ID AS postid, a.Date, a.Event_Category, a.Event_Action, a.event_name, a.hits, a.batch_id, a.siteid
                FROM pre_stage.events a
                left JOIN prod.site_archive_post b ON a.Event_Name = b.Link
                WHERE a.siteid = {id} AND a.date = '{end_date}';
            """
            cursor.execute(query2)
            print('3')
            connection.commit()
        except mysql.connector.Error as error:
            print(f"Error: {error}")

        try:
            query = f""" 
            INSERT INTO stage.events_temp_s (postid, siteid, Date, Event_Category, Event_Action, event_name, batch_id, hits)
            SELECT postid, siteid, Date, Event_Category, Event_Action, event_name, batch_id, SUM(hits)
            FROM stage.events
            WHERE siteid = {id}
            GROUP BY postid, siteid, Date, Event_Category, Event_Action, event_name, batch_id;"""
            cursor.execute(query)
            connection.commit()
        except mysql.connector.Error as error:
            print(f"Error: {error}")
        print('4')
        time.sleep(1)
      
        try:
            query3 = f"""
            INSERT INTO stage.daily_totals(SiteID, date, Visits, Unique_pageViews, batch_id )
            SELECT SiteID, date, Visits, Unique_pageViews, batch_id FROM pre_stage.daily_totals
            WHERE SiteID = {id} AND date = '{end_date}';
            """
            print('query3 ')
            # Execute the stage.daily_totals
            cursor.execute(query3)
            # Commit the changes to the database
            connection.commit()
        except mysql.connector.Error as error:
            print(f"Error: {error}")
        print('5')