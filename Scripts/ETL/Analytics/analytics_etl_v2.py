import logging
import requests
import mysql.connector
import urllib.parse
import time
import numpy as np
import json
from urllib.parse import urlparse
from urllib.parse import unquote
from datetime import datetime, timedelta,date
import os

from analytics_etl_call_stage import insert_into_stage

class Etl:
    def __init__(self):
        self.matomo_url = "https://pubmetrics.online/"
        self.readonly_token_auth = "auth"
        self.superuser_token_auth = "auth"
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)        
        self.db_data = []
        self.obj_event = []
        self.skippp = []
        self.mydata = []
        self.failed_etl = []
        self.etl_log = []
        self.pages_data = []
        self.max_batch_id = None
        self.schema = None
        self.cursor = None
        self.connection = None

      

        self.host = str(os.getenv("DB_HOST"))
        self.user = str(os.getenv("DB_USER"))
        self.password = str(os.getenv("DB_PASSWORD"))
        self.port = int(os.getenv("DB_PORT"))

        print('End_date',self.today)

    # Get pages API 
    def get_pages(self,start_date,id):
        print("get_pages_called")
        self.pages_data= []
        delta = timedelta(days=1)  # Define the step size for the loop, in this case, 1 day
        while start_date < self.today:
                
                pages_data = []
                URL = f"{self.matomo_url}index.php?module=API&method=Actions.getPageUrls&filter_limit=-1&idSite={id}&period=day&date={str(start_date)}&flat=1&format=JSON&token_auth={self.superuser_token_auth}&force_api_session=1"    
                print(URL)
                data = self.get_data(URL)
                for item in data:
                    if item is not None and len(item) > 0:
                        if item == 'result' or item == 'message':
                            continue
                        else:
                            article_id = None
                            uniq_pageviews = None
                            page_url = None
                            for key, val in item.items():
                                if key == "nb_visits":
                                    uniq_pageviews = val
                                if key == "url":
                                    page_url = val
                         
                            self.pages_data.append(([id, str(start_date),article_id, uniq_pageviews,page_url,self.max_batch_id]))       

                start_date += delta
        if len(self.pages_data) > 0:
            print("pages")
            self.insert_pages(self.pages_data)
        return pages_data

    # Get Daily_totals api
    def get_daily_totals(self,start_date,id):
        print('get_daily_totals')
        self.db_data= []
        end_date = date.today()  # Get the current date
        delta = timedelta(days=1)  # Define the step size for the loop, in this case, 1 day
        while start_date < self.today:
            URL = f"{self.matomo_url}index.php?module=API&method=VisitsSummary.get&idSite={id}&period=day&date={str(start_date)}&format=JSON&token_auth={self.superuser_token_auth}"
            print(URL)
            data = self.get_data(URL)
            nb_visits = 0
            for key, val in data.items():
                if key == "nb_visits":
                    nb_visits = val
            nb_uniq_pageviews = self.get_uniq_pageviews(id,start_date)
            if nb_visits != 0 :
                self.db_data.append(([id, str(start_date), nb_visits, nb_uniq_pageviews,self.max_batch_id]))
            start_date += delta
        if len(self.db_data) > 0:
            self.insert_daily_totals(self.db_data)
    
    # Get Unique pageViews API
    def get_uniq_pageviews(self, id,dat):
        URL = f"{self.matomo_url}index.php?module=API&method=Actions.get&idSite={id}&period=day&date={str(dat)}&format=JSON&token_auth={self.superuser_token_auth}"
        data = self.get_data(URL)
        for key, val in data.items():
            if key == "nb_uniq_pageviews":
                return val
        return 0
   
    # Get events Api
    def get_events(self,start_date,id):
        self.obj_event = []
        print('get_events')
        end_date = date.today()  # Get the current date
        delta = timedelta(days=1)  # Define the step size for the loop, in this case, 1 day
        tot = 0
        skip = 0
        ins = 0
        while start_date < self.today:
            obj_arr = []
            URL1 = f"{self.matomo_url}index.php?module=API&method=Events.getCategory&secondaryDimension=eventAction&flat=1&format=JSON&idSite={id}&period=day&date={str(start_date)}&expanded=1&token_auth={self.superuser_token_auth}"
            print(URL1)
            data1 = self.get_data(URL1)
            table = 'events'
            if len(data1) > 0:
                tot = tot + len(data1)
                for item in data1:
                    if item is not None and len(item) > 0:  
                        hits = item['nb_events']
                        ev_cat = item['Events_EventCategory']
                        sub_url = None
                        if 'Events_EventAction' in item:
                            url = item['Events_EventAction']
                            if ' | ' in url:
                                prefix, sub_url = url.split(' | ', 1)
                                parsed_url = urlparse(sub_url)
                                sub_url = parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path + parsed_url.fragment
                                ins = ins+1
                                if sub_url is None:
                                    print('this is null url')
                                    sub_url = ''
                                self.obj_event.append(([id, str(start_date), sub_url,  ev_cat, prefix, sub_url, hits, self.max_batch_id]))
                            else:
                                skip = skip+1
                                self.skippp.append(URL1)
                                self.failed_etl.append([id,'We failed to parse this json',table,self.max_batch_id])
                                continue 
                        else:
                            skip = skip+1
                            self.skippp.append(URL1)
                            self.failed_etl.append([id,'We failed to parse this json',table,self.max_batch_id])
                            continue
                                            
            start_date += delta
        if len(self.obj_event) > 0:
            # print('events',self.obj_event )
            self.insert_events(self.obj_event)

    def get_newsletter(self,start_date,id):
        try:
            print("get_newsletter")
            
            print(start_date)
            print(self.today)
            delta = timedelta(days=1)  # Define the step size for the loop, in this case, 1 day
            while start_date < self.today:
                self.compaign_data= []

                # site 17 has the dimension id of 2
                dimension = 1
                if id == 17:
                    dimension = 2

                params = {
                        'module': 'API',
                        'method': 'CustomDimensions.getCustomDimension',
                        'idSite': id ,
                        'period': 'day',
                        'date': start_date,
                        'format': 'JSON',
                        'idDimension': dimension,
                        'reportUniqueId':'CustomDimensions_getCustomDimension_idDimension--1',
                        'token_auth': self.superuser_token_auth,
                        'filter_limit': 1000,  # Number of records per request
                        'expanded':1,
                        'filter_offset': 0
                          # Specify the desired columns
                }
                all_data = []  # To store all data

                while True:
                    # Make the API request
                    api_url = self.matomo_url + "index.php?" + "&" + "&".join([f"{key}={value}" for key, value in params.items()])
                    print("API Endpoint URL:", api_url)
                    data = self.get_data(self.matomo_url, params=params)

                    # Append the data from this request to the overall data
                    all_data.extend(data)

                    # Check if there are more records to retrieve
                    if len(data) < 1000:
                        break  # Break the loop if there are no more records
                    else:
                        # Increment the offset for the next request
                        params['filter_offset'] += 1000  # Assuming you want to retrieve the next 1000 records
                try:
                    print(len(all_data))
                    for item in all_data:
                            if item is not None and len(item) > 0:
                                try:
                                    referrer_type = item['label'].split('|')[0].strip()
                                    referrer_name = None
                                    site_id = id
                                    post_id = None
                                    url = item['label'].split('|')[1].strip()
                                    visits = item['nb_visits']
                                    date = str(start_date)
                                    self.compaign_data.append([referrer_type, referrer_name, site_id, post_id, url, visits, date])
                                except: 
                                    print(f"Data Not Availble For Record In {params['filter_offset']}")
                    start_date += delta
                    if len(self.compaign_data) > 0:
                        self.insert_compaign(self.compaign_data)
                except KeyError as e:
                        print(f"KeyError occurred 3: {e}")   
                        break  
                except Exception as e:
                    print("An error occurred 2:", str(e))
                    break
        except Exception as e:
            print("An error occurred 1:", str(e))


    def insert_compaign(self, data):
            print('insert data to pre stage')
            try:
                self.connection_db()
                query = "INSERT INTO pre_stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                try:
                    self.cursor.executemany(query, data)
                    self.connection.commit()
                    self.cursor.close()
                    self.connection.close()
                    print("Data inserted successfully into Pre Stage database.")
                except Exception as error:
                    print(f"Error: {error}")
            except Exception as error:
                print("Error inserting data into MySQL database:", error)
            

    def get_idsites(self):
        URL = f"{self.matomo_url}index.php?module=API&method=SitesManager.getAllSitesId&format=JSON&token_auth={self.superuser_token_auth}"
        response = requests.get(URL)
        if response.status_code == 200:
            site_ids = response.json()
            return site_ids
        else:
            print(f"Error fetching site IDs: {response.status_code}")
            return []
        
    #  this function hit the api endpoint and get the data and change the json
    def get_data(self, url, params=None):
        response = requests.get(url, params)
        if response.status_code == 200:
            return response.json() 
        else:
            print(f"Error fetching data: {response.status_code}")
            return {}
    
    #  this is start function ,that call all function of this class    #for id in idsites:
    def start_execution(self, id):
            try:
                print('Siteid : ',id)
                print(self.today)
                print(self.yesterday)
                # exit()
                self.truncate(id)
                self.truncate_data_quality(id)
                self.connection_db()
                self.get_pages(self.yesterday,id)
                self.get_daily_totals(self.yesterday,id)
                self.get_events(self.yesterday,id)
                if id != 10:
                    self.get_newsletter(self.yesterday, id)
                    print("Newsletter")
                self.connection_db()
                insert_into_stage(self.cursor, self.connection, id)
                if id ==22:
                    self.insert_data_into_prod_string(id)
                else:
                    self.insert_data_into_prod(id)
                self.connection_db()
                self.send_data_quality(id, True)
                self.cursor.close()
                self.connection.close()
            except mysql.connector.Error as error:
                self.send_data_quality(id, False)
                print("Error inserting data into MySQL database:", error)      
        
    #  this function create the connection of database
    def connection_db(self):
        try:
            # Connect to the MySQL database
            self.connection = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
            )
            self.cursor = self.connection.cursor()
            print(' Database connection successfull')  
        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
            self.connection = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password,
            )
            self.cursor = self.connection.cursor() 

    # insert the pages api data into database
    def insert_pages(self, data):
        print('insert_pages')
        try:
            total = len(data)
            count = 0
            failed = 0
            table = 'pages'
            self.connection_db()
           
            query = "INSERT INTO pre_stage.pages (Siteid, Date, postid, Unique_pageViews, URL,  batch_id) VALUES (%s, %s, %s, %s, %s, %s)"
            #values = (id, date, page_url,  uniq_pageviews, batch_id)
            
            try:
                self.cursor.executemany(query, data)
                self.connection.commit()
                count = count + 1
            except mysql.connector.Error as error:
                failed = failed + 1
                print(f"Error: {error}")

            # Commit the changes and close the database connection
            self.etl_log.append([total,count, failed, table,self.max_batch_id])
            
            print("Data inserted successfully into Pre stage pages database.")
        except mysql.connector.Error as error:
            print("Error inserting data into Pre stage pages database:", error)
    
    def insert_events(self, obj_event):
        print('insert_events')
        total = len(obj_event)
        count = 0
        failed = 0
        table = 'events'
        self.connection_db()
        query = "INSERT INTO pre_stage.events (SiteID, Date, URL, Event_Category, Event_Action, Event_Name, Hits, batch_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        try:
            self.cursor.executemany(query, obj_event)
            self.connection.commit()
            count = count +1
            print("New record created successfully")
        except mysql.connector.Error as error:
            print(f"Error: {error}")
            failed = failed +1

        self.etl_log.append([total,count, failed, table, self.max_batch_id])
        print("Data inserted successfully into Pre stage Events database.")
     
    def truncate(self,site):
        try:
            self.connection_db()
            print('start turncate for siteid : ',site)
            truncate_query = f"DELETE FROM pre_stage.daily_totals WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM pre_stage.pages WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM pre_stage.events WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM pre_stage.newsletter WHERE site_id = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.daily_totals WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.pages WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.pages_string WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.events WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.events_string WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.events_temp_s WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.pages_temp_s WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.events_temp_string WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.pages_temp_string WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.newsletter WHERE site_id = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.newsletter_string WHERE site_id = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            self.cursor.close()
            self.connection.close()
            print('turncate end!')
        except mysql.connector.Error as error:
            print(f"Error: {error}")
            self.cursor.close()
            self.connection.close()

    # insert daily totals api data into database
    def insert_daily_totals(self, db_data):
        print('insert_daily_totals')
        total = len(db_data)
        count = 0
        failed = 0
        table = 'daily_totals'
        
        self.connection_db()
      
        query = "INSERT INTO pre_stage.daily_totals (SiteID, date, Visits, Unique_pageViews, batch_id) VALUES (%s, %s, %s, %s, %s)"
            
        try:
            self.cursor.executemany(query, db_data)
            self.connection.commit()
            count = count + 1
        except mysql.connector.Error as error:
            failed = failed + 1
            print(f"Error: {error}")
        self.etl_log.append([total,count, failed, table, self.max_batch_id])
        print("Data inserted successfully into  Pre stage daily_totals database.")

   
    def insert_data_into_prod(self, id):
        print('insert_data_into_prod')
        # sql query for aggregate of table
        query1 = f"""
        INSERT INTO prod.pages (siteid, date, postid, unique_pageviews, batch_id, URL)
        SELECT siteid, date, postid, unique_pageviews, batch_id, URL
        FROM stage.pages_temp_s
        WHERE siteid = {id} and postid is not null 
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            postid = VALUES(postid),
            unique_pageviews = VALUES(unique_pageviews),
            URL = VALUES(URL);
        """
        # Execute the pages query1
        self.cursor.execute(query1)
        # Commit the changes to the database
        self.connection.commit()
        print('1')
        time.sleep(1)

        # sql query , for aggregate of table
        delete_query =f"""delete from prod.events where date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and siteid = {id} """
        self.cursor.execute(delete_query)
        query2 = f"""
        INSERT INTO prod.events (postid, siteid, date,  Event_Category, Event_Action, event_name, hits, batch_id)
        SELECT postid, siteid, date,  Event_Category, Event_Action, event_name, hits, batch_id
        FROM stage.events_temp_s
        WHERE siteid = {id}  
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            postid = VALUES(postid),
            Event_Category = VALUES(Event_Category),
            Event_Action = VALUES(Event_Action),
            event_name = VALUES(event_name),
            hits = VALUES(hits);
        """
        # Execute the stage.events query
        self.cursor.execute(query2)
        # Commit the changes to the database
        self.connection.commit()
        print('2')
        time.sleep(1)

        query3 = f"""
        INSERT INTO prod.daily_totals (SiteID, date, Visits, Unique_pageViews, batch_id )
        SELECT SiteID, date, Visits, Unique_pageViews, batch_id 
        FROM stage.daily_totals
        WHERE siteid = {id}
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            Visits = VALUES(Visits),
            Unique_pageViews = VALUES(Unique_pageViews);
        """
        print('query3 ')
        # Execute the stage.daily_totals
        self.cursor.execute(query3)
        # Commit the changes to the database
        self.connection.commit()
        print('3')

        query4 = f"""INSERT INTO prod.newsletter (referrer_name, referrer_type, url, site_id, post_id, visits, date)
            SELECT referrer_name, referrer_type, url, site_id, post_id, visits, date
            FROM stage.newsletter
            WHERE site_id = {id}
        """
        print("query4")
        self.cursor.execute(query4)
        self.connection.commit()
        print('4')

        # close the database connection  
        self.cursor.close()
        self.connection.close()

    def insert_data_into_prod_string(self, id):
        print('insert_data_into_prod')
        # sql query for aggregate of table
        query1 = f"""
        INSERT INTO prod.pages_string (siteid, date, postid, unique_pageviews, batch_id, URL)
        SELECT siteid, date, postid, unique_pageviews, batch_id, URL
        FROM stage.pages_temp_string
        WHERE siteid = {id} and postid is not null 
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            postid = VALUES(postid),
            unique_pageviews = VALUES(unique_pageviews),
            URL = VALUES(URL);
        """
        # Execute the pages query1
        self.cursor.execute(query1)
        # Commit the changes to the database
        self.connection.commit()
        print('1')
        time.sleep(1)

        # sql query , for aggregate of table
        delete_query =f"""delete from prod.events_string where date = DATE_SUB(CURDATE(), INTERVAL 1 DAY) and siteid = {id} """
        self.cursor.execute(delete_query)
        query2 = f"""
        INSERT INTO prod.events_string (postid, siteid, date,  Event_Category, Event_Action, event_name, hits, batch_id)
        SELECT postid, siteid, date,  Event_Category, Event_Action, event_name, hits, batch_id
        FROM stage.events_temp_string
        WHERE siteid = {id}  
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            postid = VALUES(postid),
            Event_Category = VALUES(Event_Category),
            Event_Action = VALUES(Event_Action),
            event_name = VALUES(event_name),
            hits = VALUES(hits);
        """
        # Execute the stage.events query
        self.cursor.execute(query2)
        # Commit the changes to the database
        self.connection.commit()
        print('2')
        time.sleep(1)

        query3 = f"""
        INSERT INTO prod.daily_totals (SiteID, date, Visits, Unique_pageViews, batch_id )
        SELECT SiteID, date, Visits, Unique_pageViews, batch_id 
        FROM stage.daily_totals
        WHERE siteid = {id}
        ON DUPLICATE KEY UPDATE
            siteid = VALUES(siteid),
            date = VALUES(date),
            Visits = VALUES(Visits),
            Unique_pageViews = VALUES(Unique_pageViews);
        """
        print('query3 ')
        # Execute the stage.daily_totals
        self.cursor.execute(query3)
        # Commit the changes to the database
        self.connection.commit()
        print('3')

        query4 = f"""INSERT INTO prod.newsletter_string (referrer_name, referrer_type, url, site_id, post_id, visits, date)
            SELECT referrer_name, referrer_type, url, site_id, post_id, visits, date
            FROM stage.newsletter_string
            WHERE site_id = {id}
        """
        print("query4")
        self.cursor.execute(query4)
        self.connection.commit()
        print('4')

        # close the database connection  
        self.cursor.close()
        self.connection.close()





    def truncate_data_quality(self, site):

        self.connection_db()
        cursor = self.cursor
        connection = self.connection

        current_date = (datetime.now()).strftime('%Y-%m-%d')
        print(current_date)

        sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND swagger_status IS NULL AND traffic_status IS NULL AND cms_status IS NULL AND traffic_ingestion_status IS NULL and siteid = {site};"""
        
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
            INSERT INTO prod.posting_status (siteid, posting_date, analytics_status)
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

def dev_etl(id):
    etl = Etl()
    logging.info(etl.matomo_url)
    etl.start_execution(id)