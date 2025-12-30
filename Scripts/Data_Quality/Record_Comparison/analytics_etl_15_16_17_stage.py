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


        remove_param_page= f"""
        UPDATE pre_stage.pages a
        SET a.URL = 
            CASE
                WHEN a.URL REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.URL, '?', 1)
                ELSE a.URL
            END
        WHERE a.SITEID = {id};

        """

        cursor.execute(remove_param_page)
        connection.commit()

        try:
            query1 = f"""        
                INSERT INTO stage.pages (postid, siteid, date, URL, unique_pageviews, batch_id)
                SELECT b.ID AS postid, a.siteid, a.date, a.URL, a.unique_pageviews, a.batch_id
                FROM pre_stage.pages a
                LEFT JOIN prod.site_archive_post b ON a.URL = b.Link
                WHERE  a.siteid = {id};
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