import json
import logging
import os
import requests
import mysql.connector
from datetime import datetime
from datetime import datetime, timedelta,date
from urllib.parse import urlparse
from urllib.parse import unquote
import json
import requests
import mailst
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class Etl:
    def __init__(self):
        self.matomo_url = "https://pubmetrics.online/index.php?"
        self.token_auth = "auth"
        self.today = date.today()
        # Calculate yesterday's date
        self.yesterday = self.today - timedelta(days=1)   
#         self.today= self.today - timedelta(days=1) 
        self.cursor = None
        self.connection = None
        self.max_batch_id = None
        self.compaign_data =[]
        
        self.host = os.getenv("DB_HOST")
        self.user= os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = int(os.getenv("DB_PORT"))
        #PROD
       
    # Get pages API 
        # Get pages API 
    def get_compaign(self,start_date,id):
        try:
            print("get_compaign")
            
            print(start_date)
            print(self.today)
            delta = timedelta(days=1)  # Define the step size for the loop, in this case, 1 day
            while start_date < self.today:
                self.compaign_data= []
                params = {
                        'module': 'API',
                        'method': 'Live.getLastVisitsDetails',
                        'idSite': id ,
                        'period': 'day',
                        'date': start_date,
                        'format': 'JSON',
                        'token_auth': self.token_auth,
                        'filter_limit': 1000,  # Number of records per request
                        'filter_offset': 0  # Initial offset
                          # Specify the desired columns
                }
                all_data = []  # To store all data

                while True:
                    # Make the API request
                    api_url = self.matomo_url + "&" + "&".join([f"{key}={value}" for key, value in params.items()])
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
                    count = 0
                    for item in all_data:
                            if item is not None and len(item) > 0:
                                    if item.get('actionDetails'): 
                                        server_date = datetime.strptime(item['actionDetails'][0]['serverTimePretty'],  '%b %d, %Y %H:%M:%S')
                                        date = server_date.strftime('%Y-%m-%d')
                                        hour = server_date.strftime('%H')
                                        server_date =  item['actionDetails'][0]['serverTimePretty']
                                        siteid =  item['idSite']
                                        Referrer_Type = item['referrerType'].strip()
                                        Referrer_Name= item['referrerName'] 
                                        Referrer_Keyword = item['referrerKeyword'] if Referrer_Type == 'campaign' else ''
                                        action_details = item['actionDetails']
                                        firstURL = next((action['url'] for action in action_details if action['type'] == 'action'), '')
                                        parsed_url = urlparse(firstURL)
                                        first_URL = parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path
                                        self.compaign_data.append(([id, server_date,first_URL,Referrer_Type, Referrer_Name,Referrer_Keyword,date,hour,self.max_batch_id]))  
                        
                                # print(id, server_date,first_URL,Referrer_Type, Referrer_Name,Referrer_Keyword,date,hour)
                    start_date += delta
                    if len(self.compaign_data) > 0:
                        self.insert_compaign(self.compaign_data)
                except KeyError as e:
                        print(f"KeyError occurred: {e}")   
                        break  
                except Exception as e:
                    print("An error occurred:", str(e))
                    break
            
        except Exception as e:
            print("An error occurred:", str(e))
    def get_idsites(self):
        URL = f"{self.matomo_url}module=API&method=SitesManager.getAllSitesId&format=JSON&token_auth={self.token_auth}"
        response = requests.get(URL)
        if response.status_code == 200:
            site_ids = response.json()
            return site_ids
        else:
            print(f"Error fetching site IDs: {response.status_code}")
            return []
        
    #  this function hit the api endpoint and get the data and change the json
    def get_data(self, url,params):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = json.loads(response.text)
            return data 
        else:
            print(f"Error fetching data: {response.status_code}")
            return {}
    def connection_db(self):
        try:
            # Connect to the MySQL database
            self.connection = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password
            )
            self.cursor = self.connection.cursor()

            print('successfull')

            
        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
        # its create and add the batch details 
    def batch_insert(self):
        print('batch_insert')
        try:
            query = "INSERT INTO stage.batch_name (Type, status) VALUES (%s, %s)"
            values = ('Analystics Traffic Channels Data', 'initialize')
            self.cursor.execute(query, values)
            self.connection.commit()
            
            # Define the SQL query
            query = "SELECT MAX(id) AS max_id FROM stage.batch_name;"

            # Execute the query
            self.cursor.execute(query)

            # Fetch the result
            result = self.cursor.fetchone()

            # Access the maximum id value
            self.max_batch_id = result[0]

        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
    def comp_batch_insert(self):
        self.connection_db()
       
        try:
            query = "INSERT INTO stage.batch_name (Type, status) VALUES (%s, %s)"
            values = ('Analystics Traffic Channels Data', 'Completed')
            self.cursor.execute(query, values)
            self.connection.commit()

        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
    
            
    def insert_compaign(self, data):
        print('insert data to pre stage traffic_channels ')
        try:
            total = len(data)
            count = 0
            failed = 0
            table = 'pages'
            self.connection_db()
           
            query = "INSERT INTO pre_stage.traffic_channels (siteid, server_date, firsturl, referrer_type, referrer_name, referrer_keyword, date, hour, batch_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)"
            
            try:
                self.cursor.executemany(query, data)
                self.connection.commit()
                self.cursor.close()
                self.connection.close()
                #count = count + 1
                #print("New record created successfully")
            except mysql.connector.Error as error:
                #failed = failed + 1
                self.cursor.executemany(query, data)
                self.connection.commit()
                self.cursor.close()
                self.connection.close()
                print(f"Error: {error}")

            # Commit the changes and close the database connection

            #self.etl_log.append([total,count, failed, table,self.max_batch_id])
            #print(self.etl_log)
            
            print("Data inserted successfully into Pre Stage database.")
        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
    def truncate(self,site):
        try:
            self.connection_db()
            print('start turncate for siteid : ',site)
            truncate_query = f"DELETE FROM pre_stage.traffic_channels WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.traffic_channels WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
           
            # time.sleep(10)
            # close the database connection  
            self.cursor.close()
            self.connection.close()
            
        except mysql.connector.Error as error:
            print(f"Error: {error}")
            
    def insert_data_into_stage(self,siteid,domain):
        print('Data insertion start into Stage')
        try:
            if siteid == 14:
                query = f"""INSERT INTO stage.traffic_channels (server_date,postid, siteid, date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, batch_id)
                    SELECT a.server_date,b.ID AS postid, a.siteid, a.date, a.hour, a.firsturl, a.referrer_type, a.referrer_name , a.referrer_keyword, a.batch_id
                    FROM pre_stage.traffic_channels  a
                    LEFT JOIN prod.site_archive_post b ON a.firsturl = concat(b.Link,'/')
                    WHERE  a.siteid = {siteid};"""
            else:
                 query = f"""INSERT INTO stage.traffic_channels (server_date,postid, siteid, date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, batch_id)
                    SELECT a.server_date,b.ID AS postid, a.siteid, a.date, a.hour, a.firsturl, a.referrer_type, a.referrer_name , a.referrer_keyword, a.batch_id
                    FROM pre_stage.traffic_channels  a
                    LEFT JOIN prod.site_archive_post b ON a.firsturl = b.Link
                    WHERE  a.siteid = {siteid};"""

            
            update_link_query14 =f"""
                UPDATE pre_stage.traffic_channels a
                        SET a.firsturl = 
                            CASE
                                WHEN a.firsturl REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.firsturl, '?', 1)
                                ELSE a.firsturl
                            END
                        WHERE a.SITEID = {siteid}
                """
            
            remove_article_query14=f"""
                update pre_stage.traffic_channels SET firsturl = REPLACE(firsturl, '/artikel', '')
                WHERE firsturl LIKE '%/artikel%' and siteid ={siteid};
                """
            if siteid == 12 or siteid == 14:
                print('insert_data_into_stage for site 12')
                query1 = f"""
                UPDATE stage.traffic_channels
                SET RealReferrer = 
                    CASE
                         -- Direct
                        WHEN firsturl = '{domain}'
                            OR (referrer_type = 'direct' AND postid  is null)
                            THEN 'Direct'
                        -- edge
                        WHEN referrer_type = 'search' AND referrer_name = 'Facebook'
                            THEN 'Facebook'
                        WHEN referrer_type = 'search' AND referrer_name = 'LinkedIn'
                            THEN 'LinkedIn'
                        WHEN referrer_type = 'search' AND referrer_name = 'Twitter'
                            THEN 'X'
                        WHEN referrer_type = 'search' AND referrer_name = 'Instagram'
                            THEN 'Instagram'
                        -- Search
                        WHEN referrer_type = 'search' AND firsturl <> '{domain}'
                            THEN 'Search'

                        -- Facebook
                        WHEN referrer_name = 'Facebook' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'FB')
                            THEN 'Facebook'
                        -- LinkedIn
                        WHEN referrer_name = 'LinkedIn' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'LI')
                            THEN 'LinkedIn'

                        -- X
                        WHEN referrer_name = 'Twitter' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'X')
                            THEN 'X'

                        -- Instagram
                        WHEN referrer_name = 'Instagram' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'IN')
                            THEN 'Instagram'

                        -- Websites
                        WHEN referrer_type = 'website' AND firsturl <> '{domain}'
                            THEN 'Websites'

                        -- Unknown
                        WHEN referrer_type = 'direct' AND  postid is not null
                            THEN 'Unknown'

                        -- Newsletter
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a'
                            THEN 'NL <47'
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b'
                            THEN 'NL 48-60'
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c'
                            THEN 'NL >60'

                        -- Other
                        ELSE 'Other'
                        END
                        where siteid = {siteid};
                """ 
                # in
            elif siteid == 13:
                print('insert_data_into_stage for site 13')

                query1 = f"""
           UPDATE stage.traffic_channels
            SET RealReferrer = 
                CASE
                     -- Direct
                    WHEN firsturl = '{domain}'
                        OR (referrer_type = 'direct' AND postid  is null)
                        THEN 'Direct'
                    -- edge
                    WHEN referrer_type = 'search' AND referrer_name = 'Facebook'
                        THEN 'Facebook'
                    WHEN referrer_type = 'search' AND referrer_name = 'LinkedIn'
                        THEN 'LinkedIn'
                    WHEN referrer_type = 'search' AND referrer_name = 'Twitter'
                        THEN 'X'
                    WHEN referrer_type = 'search' AND referrer_name = 'Instagram'
                        THEN 'Instagram'
                    -- Search
                    WHEN referrer_type = 'search' AND firsturl <> '{domain}'
                        THEN 'Search'

                    -- Facebook
                    WHEN referrer_name = 'Facebook' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'FB')
                        THEN 'Facebook'
                    -- LinkedIn
                    WHEN referrer_name = 'LinkedIn' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'LI')
                        THEN 'LinkedIn'

                    -- X
                    WHEN referrer_name = 'Twitter' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'X')
                        THEN 'X'

                    -- Instagram
                    WHEN referrer_name = 'Instagram' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'IN')
                        THEN 'Instagram'

                    -- Websites
                    WHEN referrer_type = 'website' AND firsturl <> '{domain}'
                        THEN 'Websites'

                    -- Unknown
                    WHEN referrer_type = 'direct' AND  postid is not null
                        THEN 'Unknown'

                    -- Newsletter
                    WHEN referrer_type = 'campaign' AND referrer_name = 'mail*'
                            THEN 'Newsletter'
                    WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a'
                            THEN 'NL Medlem'
                    WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b'
                            THEN 'NL fra NNF'

                    -- Other
                    ELSE 'Other'
                    END
                    where siteid = {siteid};
                """ 
            elif siteid == 16:
                print('insert_data_into_stage for site 16')
                query1 = f"""
                UPDATE stage.traffic_channels
                SET RealReferrer = 
                    CASE
                         -- Direct
                        WHEN firsturl = '{domain}'
                            OR (referrer_type = 'direct' AND postid  is null)
                            THEN 'Direct'
                        -- edge
                        WHEN referrer_type = 'search' AND referrer_name = 'Facebook'
                            THEN 'Facebook'
                        WHEN referrer_type = 'search' AND referrer_name = 'LinkedIn'
                            THEN 'LinkedIn'
                        WHEN referrer_type = 'search' AND referrer_name = 'Twitter'
                            THEN 'X'
                        WHEN referrer_type = 'search' AND referrer_name = 'Instagram'
                            THEN 'Instagram'
                        -- Search
                        WHEN referrer_type = 'search' AND firsturl <> '{domain}'
                            THEN 'Search'

                        -- Facebook
                        WHEN referrer_name = 'Facebook' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'FB')
                            THEN 'Facebook'
                        -- LinkedIn
                        WHEN referrer_name = 'LinkedIn' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'LI')
                            THEN 'LinkedIn'

                        -- X
                        WHEN referrer_name = 'Twitter' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'X')
                            THEN 'X'

                        -- Instagram
                        WHEN referrer_name = 'Instagram' AND firsturl <> '{domain}'
                            OR (referrer_type = 'campaign' AND referrer_name = 'IN')
                            THEN 'Instagram'

                        -- Websites
                        WHEN referrer_type = 'website' AND firsturl <> '{domain}'
                            THEN 'Websites'

                        -- Unknown
                        WHEN referrer_type = 'direct' AND  postid is not null
                            THEN 'Unknown'

                        -- Newsletter
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a'
                            THEN 'NL'
                        WHEN referrer_type = 'campaign' AND referrer_name like 'mail%'
                            THEN 'Newsletter'

                        -- Other
                        ELSE 'Other'
                        END
                        where siteid = {siteid};
                """ 
            elif siteid == 17:
                print('insert_data_into_stage for site 17')
                query1 = f"""
            UPDATE stage.traffic_channels
            SET RealReferrer = 
                CASE
                     -- Direct
                    WHEN firsturl = '{domain}'
                        OR (referrer_type = 'direct' AND postid  is null)
                        THEN 'Direct'
                    -- edge
                    WHEN referrer_type = 'search' AND referrer_name = 'Facebook'
                        THEN 'Facebook'
                    # WHEN referrer_type = 'search' AND referrer_name = 'LinkedIn'
                    #     THEN 'LinkedIn'
                    # WHEN referrer_type = 'search' AND referrer_name = 'Twitter'
                    #     THEN 'X'
                    # WHEN referrer_type = 'search' AND referrer_name = 'Instagram'
                    #     THEN 'Instagram'
                    -- Search
                    WHEN referrer_type = 'search' AND firsturl <> '{domain}'
                        THEN 'Search'

                    -- Facebook
                    WHEN referrer_name = 'Facebook' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'FB')
                        THEN 'Facebook'
                    # -- LinkedIn
                    # WHEN referrer_name = 'LinkedIn' AND firsturl <> '{domain}'
                    #     OR (referrer_type = 'campaign' AND referrer_name = 'LI')
                    #     THEN 'LinkedIn'

                    # -- X
                    # WHEN referrer_name = 'Twitter' AND firsturl <> '{domain}'
                    #     OR (referrer_type = 'campaign' AND referrer_name = 'X')
                    #     THEN 'X'

                    # -- Instagram
                    # WHEN referrer_name = 'Instagram' AND firsturl <> '{domain}'
                    #     OR (referrer_type = 'campaign' AND referrer_name = 'IN')
                    #     THEN 'Instagram'

                    -- Websites
                    WHEN referrer_type = 'website' AND firsturl <> '{domain}'
                        THEN 'Websites'

                    -- Unknown
                    WHEN referrer_type = 'direct' AND  postid is not null
                        THEN 'Unknown'

                    -- Newsletter
                    WHEN referrer_type = 'campaign' AND referrer_name like 'mail%'
                        THEN 'Newsletter'

                    -- Other
                    ELSE 'Other'
                    END
                    where siteid = {siteid};
            """ 
            else:
                query1 = f"""
            UPDATE stage.traffic_channels
            SET RealReferrer = 
                CASE
                     -- Direct
                    WHEN firsturl = '{domain}'
                        OR (referrer_type = 'direct' AND postid  is null)
                        THEN 'Direct'
                    -- edge
                    WHEN referrer_type = 'search' AND referrer_name = 'Facebook'
                        THEN 'Facebook'
                    WHEN referrer_type = 'search' AND referrer_name = 'LinkedIn'
                        THEN 'LinkedIn'
                    WHEN referrer_type = 'search' AND referrer_name = 'Twitter'
                        THEN 'X'
                    WHEN referrer_type = 'search' AND referrer_name = 'Instagram'
                        THEN 'Instagram'
                    -- Search
                    WHEN referrer_type = 'search' AND firsturl <> '{domain}'
                        THEN 'Search'

                    -- Facebook
                    WHEN referrer_name = 'Facebook' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'FB')
                        THEN 'Facebook'
                    -- LinkedIn
                    WHEN referrer_name = 'LinkedIn' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'LI')
                        THEN 'LinkedIn'

                    -- X
                    WHEN referrer_name = 'Twitter' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'X')
                        THEN 'X'

                    -- Instagram
                    WHEN referrer_name = 'Instagram' AND firsturl <> '{domain}'
                        OR (referrer_type = 'campaign' AND referrer_name = 'IN')
                        THEN 'Instagram'

                    -- Websites
                    WHEN referrer_type = 'website' AND firsturl <> '{domain}'
                        THEN 'Websites'

                    -- Unknown
                    WHEN referrer_type = 'direct' AND  postid is not null
                        THEN 'Unknown'

                    -- Newsletter
                    WHEN referrer_type = 'campaign' AND referrer_name like 'mail%'
                        THEN 'Newsletter'

                    -- Other
                    ELSE 'Other'
                    END
                    where siteid = {siteid};
            """ 
            query2 = f"""INSERT INTO stage.traffic_channels_v2 (server_date,postid, siteid, date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, batch_id)
            select server_date,postid, siteid, date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, batch_id from stage.traffic_channels where siteid = {siteid};"""   

            try:
                self.connection_db() 
                if siteid == 14:
                    self.cursor.execute(update_link_query14)
                    self.connection.commit()
                    self.cursor.execute(remove_article_query14)
                    self.connection.commit()
                self.cursor.execute(update_link_query14)
                self.cursor.execute(query)
                self.connection.commit()
                self.cursor.execute(query1)
                self.connection.commit()
                self.cursor.execute(query2)
                self.connection.commit()

            except mysql.connector.Error as error:
                self.connection_db() 
                self.cursor.execute(query)
                self.connection.commit()
                self.cursor.execute(query1)
                self.connection.commit()
                self.cursor.execute(query2)
                self.connection.commit()
                print(f"Error: {error}")
            self.connection.close()
            print("Insertion complete into Stage")
        except Exception as e:
            print("An error occurred:", str(e))
        
        #  this is start function ,that call all function of this class
    def insert_data_into_prod(self,siteid):
        print('start insertion into Production')
        # Aggregation 1
        if siteid == 12 or siteid == 14:
            print('delete data for site 12')
            query_delete = f"""delete  from prod.entry_pages_agg WHERE
                Siteid = {siteid} and date = '{self.yesterday}'     """
            print('insert_data_into_stage for site 12')
            query = f"""INSERT INTO prod.entry_pages_agg (
            Siteid, Date, RealReferrer, referrer_name, referrer_keyword, firsturl, hits
        )
	WITH conditions AS (
        SELECT
            siteid, date,RealReferrer, 
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_name
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_name
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_name
				ELSE NULL
			END AS referrer_name,  
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_keyword
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_keyword
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_keyword
				ELSE NULL
			END AS referrer_keyword,
            firsturl
        FROM stage.traffic_channels
            WHERE
                Siteid = {siteid}
                )
                
		SELECT siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl,  COUNT(*) as Hits
        FROM conditions 
        GROUP BY siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl;
      ;"""
            print('delete data for site 12')
            query1_delete = f"""delete  from prod.visits_rythm WHERE
                Siteid = {siteid} and date = '{self.yesterday}'     """
            query1 = f"""INSERT INTO prod.visits_rythm (Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, Hits)
            WITH conditions AS (
                SELECT
                    Siteid, Date, Hour, RealReferrer, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_name
                        ELSE NULL
                    END AS referrer_name, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_keyword
                        ELSE NULL
                    END AS referrer_keyword,
                    CASE
                        WHEN DAYOFWEEK(Date) = 1 THEN 'Monday'
                        WHEN DAYOFWEEK(Date) = 2 THEN 'Tuesday'
                        WHEN DAYOFWEEK(Date) = 3 THEN 'Wednesday'
                        WHEN DAYOFWEEK(Date) = 4 THEN 'Thursday'
                        WHEN DAYOFWEEK(Date) = 5 THEN 'Friday'
                        WHEN DAYOFWEEK(Date) = 6 THEN 'Saturday'
                        WHEN DAYOFWEEK(Date) = 7 THEN 'Sunday'
                        ELSE 'Unknown'
                    END AS Weekday
                FROM stage.traffic_channels
                WHERE Siteid ={siteid}
            )
        SELECT Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, COUNT(*) as Hits
        FROM conditions 
        GROUP BY Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday;"""
        elif siteid == 13:
            print('delete data for site 13')
            query_delete = f"""delete  from prod.entry_pages_agg WHERE
                Siteid = {siteid} and date = '{self.yesterday}'     """
            print('insert_data_into_stage for site 13')
            query = f"""INSERT INTO prod.entry_pages_agg (
            Siteid, Date, RealReferrer, referrer_name, referrer_keyword, firsturl, hits
        )
	WITH conditions AS (
        SELECT
            siteid, date,RealReferrer, 
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'mail*' THEN referrer_name
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_name
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_name
				ELSE NULL
			END AS referrer_name,  
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'mail*' THEN referrer_keyword
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_keyword
                WHEN referrer_type = 'campaign' AND referrer_name = 'mail-c' THEN referrer_keyword
				ELSE NULL
			END AS referrer_keyword,
            firsturl
        FROM stage.traffic_channels
            WHERE
                Siteid = {siteid}
                )
                
		SELECT siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl,  COUNT(*) as Hits
        FROM conditions 
        GROUP BY siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl;
      ;"""
            print('delete data for site 13')
            query1_delete = f"""delete  from prod.visits_rythm WHERE
                Siteid = {siteid} and date = '{self.yesterday}'     """
            query1 = f"""INSERT INTO prod.visits_rythm (Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, Hits)
            WITH conditions AS (
                SELECT
                    Siteid, Date, Hour, RealReferrer, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail*' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_name
                        ELSE NULL
                    END AS referrer_name, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail*' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-a' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'mail-b' THEN referrer_keyword
                        ELSE NULL
                    END AS referrer_keyword,
                    CASE
                        WHEN DAYOFWEEK(Date) = 1 THEN 'Monday'
                        WHEN DAYOFWEEK(Date) = 2 THEN 'Tuesday'
                        WHEN DAYOFWEEK(Date) = 3 THEN 'Wednesday'
                        WHEN DAYOFWEEK(Date) = 4 THEN 'Thursday'
                        WHEN DAYOFWEEK(Date) = 5 THEN 'Friday'
                        WHEN DAYOFWEEK(Date) = 6 THEN 'Saturday'
                        WHEN DAYOFWEEK(Date) = 7 THEN 'Sunday'
                        ELSE 'Unknown'
                    END AS Weekday
                FROM stage.traffic_channels
                WHERE Siteid ={siteid}
            )
        SELECT Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, COUNT(*) as Hits
        FROM conditions 
        GROUP BY Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday;"""
        else:
            print('delete data for sites')
            query_delete = f"""delete  from prod.entry_pages_agg WHERE
                Siteid = {siteid} and date = '{self.yesterday}'    """
            query = f"""INSERT INTO prod.entry_pages_agg (
            Siteid, Date, RealReferrer, referrer_name, referrer_keyword, firsturl, hits
        )
	WITH conditions AS (
        SELECT
            siteid, date,RealReferrer, 
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
				WHEN referrer_type = 'campaign' AND referrer_name LIKE 'mail%' THEN referrer_name
				ELSE NULL
			END AS referrer_name,  
            CASE 
				WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
				WHEN referrer_type = 'campaign' AND referrer_name LIKE 'mail%' THEN referrer_keyword
				ELSE NULL
			END AS referrer_keyword,
            firsturl
        FROM stage.traffic_channels
            WHERE
                Siteid = {siteid}
                )
                
		SELECT siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl,  COUNT(*) as Hits
        FROM conditions 
        GROUP BY siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl;
      ;"""
            print('delete data for sites')
            query1_delete = f"""delete  from prod.visits_rythm WHERE
                Siteid = {siteid} and date = '{self.yesterday}'"""
            query1 = f"""INSERT INTO prod.visits_rythm (Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, Hits)
            WITH conditions AS (
                SELECT
                    Siteid, Date, Hour, RealReferrer, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_name
                        WHEN referrer_type = 'campaign' AND referrer_name LIKE 'mail%' THEN referrer_name
                        ELSE NULL
                    END AS referrer_name, 
                    CASE 
                        WHEN referrer_type = 'campaign' AND referrer_name = 'FB' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'IN' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'LI' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name = 'X' THEN referrer_keyword
                        WHEN referrer_type = 'campaign' AND referrer_name LIKE 'mail%' THEN referrer_keyword
                        ELSE NULL
                    END AS referrer_keyword,
                    CASE
                        WHEN DAYOFWEEK(Date) = 1 THEN 'Monday'
                        WHEN DAYOFWEEK(Date) = 2 THEN 'Tuesday'
                        WHEN DAYOFWEEK(Date) = 3 THEN 'Wednesday'
                        WHEN DAYOFWEEK(Date) = 4 THEN 'Thursday'
                        WHEN DAYOFWEEK(Date) = 5 THEN 'Friday'
                        WHEN DAYOFWEEK(Date) = 6 THEN 'Saturday'
                        WHEN DAYOFWEEK(Date) = 7 THEN 'Sunday'
                        ELSE 'Unknown'
                    END AS Weekday
                FROM stage.traffic_channels
                WHERE Siteid ={siteid}
            )
        SELECT Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, COUNT(*) as Hits
        FROM conditions 
        GROUP BY Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday;"""
        print(f'delete data for site {siteid}')
        query2_delete = f"""delete  from prod.realreferrer WHERE
                Siteid = {siteid} and date = '{self.yesterday}'     """
        query2 = f"""INSERT INTO  prod.realreferrer (Siteid, Date, RealReferrer, Hits)
            SELECT Siteid, Date, RealReferrer, COUNT(*) as hits
            FROM   stage.traffic_channels where siteid = {siteid}
            GROUP BY Siteid, Date, RealReferrer;
            """
        query3 = f"""
        INSERT INTO prod.traffic_channels (SiteID, postid , date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, RealReferrer, batch_id )
        SELECT SiteID, postid , date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, RealReferrer, batch_id FROM stage.traffic_channels
        where siteid = {siteid};"""
        query3_delete = f"""
        delete  from prod.traffic_channels  where siteid = {siteid} and date = '{self.yesterday}';
       """
        try:
            self.connection_db() 
            print(query_delete)
            
            self.cursor.execute(query_delete)

            self.connection.commit()
            print('fucking comit')
            self.cursor.execute(query)
            self.connection.commit()
            print('1')
            self.cursor.execute(query1_delete)
            self.connection.commit()
            self.cursor.execute(query1)
            self.connection.commit()
            print('2')
            self.cursor.execute(query2_delete)
            self.connection.commit()
            self.cursor.execute(query2)
            self.connection.commit()
            print('3')
            self.cursor.execute(query3_delete)
            self.connection.commit()
            print('4')
            self.cursor.execute(query3)
            self.connection.commit()
            print('5')
            
        except mysql.connector.Error as error:
            self.connection_db() 
            self.cursor.execute(query)
            print(f"Error: {error}")
            self.connection.commit()
            self.cursor.execute(query1)
            self.connection.commit()
            self.cursor.execute(query2)
            self.connection.commit()
            self.cursor.execute(query3)
            self.connection.commit()
            print(f"Error: {error}")
        self.connection.close()
        print('Completed insertion!')
        
       # dq 
    def Data_Quality(self,siteid):
        try:
            print('Data_Quality')
            target=0
            stop_on_failure= 1
            name ='traffic_channel_n_daily_totals_count_DQ'
            query = f'call prod.traffic_channel_n_daily_totals_count_DQ({siteid});'
            try:  
                self.connection_db() 
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                self.cursor.close()
                self.connection.close()
                result_value =0
            except Exception as e:
                print(e)
                self.connection_db() 
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                self.cursor.close()
                self.connection.close()
                result_value =0

            if result is not None and result[0] is not None and result[0] > 0:
                print(f"Query result for '{name}': {result[0]}")
                result_value = result[0]
                try:
                    ETL_name = "Trafikkanaler (Traffic Channels)"  # Modify this with your actual Swagger link
                    # Insert the result into the prod.dq_checks table
                    subject = f'[ISSUE][{ETL_name}][DQ_failed][DQ_name : {name}][Siteid : {siteid}] Data Quality'
                    message = f"Hello ! "
                    message = str(message)
                    email_template = f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <style>
                                .failed-list {{
                                    border-collapse: collapse;
                                    width: 100%;
                                    border: 1px solid black;
                                }}

                                .failed-list th, .failed-list td {{
                                    border: 1px solid black;
                                    padding: 8px;
                                    text-align: left;
                                }}

                                .failed-list th {{
                                    background-color: #f2f2f2;
                                }}
                            </style>
                        </head>
                        <body>
                            <div class="email-container">
                                <div class="header">Data Quality Check Report: Failed Entries</div>

                                <div class="details">
                                    Dear Team,
                                    <p>We are writing to inform you about recent data quality check failures that require your attention. Our data quality monitoring process has identified issues in the following entries: An important update the Data Quality check {name} for Site ID {siteid} has failed. Immediate action is needed to resolve this issue.</p>
                                </div>

                                <table class="failed-list">
                                    <tr>
                                        <th>siteId</th>
                                        <th>name</th>
                                        <th>value</th>
                                        <th>target</th>
                                        <th>result</th>
                                        <th>Stop on failure</th>
                                        <th>source</th>

                                    </tr>
                                    <tr>
                                        <td><strong>{siteid}</strong></td>
                                        <td>{name}</td>
                                        <td>{result[0]}</td>
                                        <td>{target}</td>
                                        <td>{result_value}</td>
                                        <td>{stop_on_failure}</td>
                                        <td>{ETL_name}</td>

                                    </tr>

                                    <!-- Add more failed entries as needed -->
                                </table>
                                <br>

                                <hr>
                                <p><strong><b>Server Resources</b></strong></p>

                                <div class="details">
                                    <p>We kindly request that you review and address these issues promptly to ensure the accuracy and reliability of our data. If you have any questions or need further assistance, please don't hesitate to reach out to our support team.</p>

                                    <p>Thank you for your attention to this matter.</p>

                                    <p>Sincerely,
                                    <br>[Gulraiz]
                                    <br>[Dotlabs]
                                    <br>[https://sindri.media/]</p>
                                </div>
                            </div>
                        </body>
                        </html>
                        """
                    insert_query = """
                    INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (result[0], name, stop_on_failure, target, result_value, ETL_name,siteid)
                    
                    try:
                        self.connection_db()
                        self.cursor.execute(insert_query, values)
                        self.connection.commit()
                        self.connection.close()
                        try:
                            mailst.mailConnection(subject, message,email_template) 
                        except Exception as e:
                            mailst.mailConnection(subject, message,email_template)
                            print(e)

                    except Exception as e:
                        print(e)
                        self.connection_db()
                        self.cursor.execute(insert_query, values)
                        self.connection.commit()
                        self.connection.close()
                        try:
                            mailst.mailConnection(subject, message,email_template) 
                        except Exception as e:
                            mailst.mailConnection(subject, message,email_template)
                            print(e)
                except Exception as e:
                    print(e)    
            else:
                insert_query = """
                INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                values = (0, name, 0, 0, 0,  "Trafikkanaler (Traffic Channels) Data DQ complete",siteid)
                try:
                    self.connection_db()
                    self.cursor.execute(insert_query, values)
                    self.connection.commit()
                    self.connection.close()

                except Exception as e:
                    print(e)
                    self.connection_db()
                    self.cursor.execute(insert_query, values)
                    self.connection.commit()
                    self.connection.close()
                
        except Exception as e:
                print(e)
    def start_execution(self):

        idsites = self.get_idsites()
        # idsites = [12]
        self.connection_db()
        self.batch_insert()
        self.connection.close()
        for id in idsites:
            try: 
                
                if id == 11 or id == 15:
                    print('siteid is:', id)
                    pass 
                else:
                    print('Siteid : ',id)
                    self.truncate(id)
                    self.connection_db()
                    query = "INSERT INTO stage.completion_log (siteid, Type, status,batch_id) VALUES (%s, %s, %s, %s)"
                    values = (id, 'Analystics Trafikkanaler', 'initialize',self.max_batch_id)
                    self.cursor.execute(query, values)
                    self.connection.commit()
                    self.get_compaign(self.yesterday,id)
                    self.connection_db()
                    query_do = f"SELECT domain FROM  prod.domain_siteid  where siteid = {id}"
                    self.cursor.execute(query_do)
                    rows = self.cursor.fetchall()
                    domain = rows[0][0]
                    print('Domain: ',domain)
                    self.cursor.close()
                    self.connection.close()
                    self.insert_data_into_stage(id,domain)
                    self.insert_data_into_prod(id)
                    self.Data_Quality(id)
                    self.connection_db()
                    query = "INSERT INTO stage.completion_log (siteid, Type, status,batch_id) VALUES (%s, %s, %s, %s)"
                    values = (id,'Analystics Trafikkanaler', 'Completed',self.max_batch_id)
                    self.cursor.execute(query, values)
                    self.connection.commit()
                    self.cursor.close()
                    self.connection.close()
            except mysql.connector.Error as error:
                print("Error inserting data into MySQL database:", error)
        self.comp_batch_insert()

      

# create a object of class
def dev_etl():
    etl = Etl()
    logging.info(etl.matomo_url)
    etl.start_execution()