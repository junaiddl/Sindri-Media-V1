import json
import logging
import requests
import mysql.connector
from datetime import datetime
from datetime import datetime, timedelta,date
from urllib.parse import urlparse
from urllib.parse import unquote
import json
import requests
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)
class Etl:
    def __init__(self):
        self.matomo_url = "https://pubmetrics.online/index.php?"
        self.token_auth = "auth"
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)   
        self.cursor = None
        self.connection = None
        self.max_batch_id = None
        self.compaign_data =[]
        self.siteid = 13

        self.host = os.getenv("DB_HOST")
        self.user= os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = int(os.getenv("DB_PORT"))
        
  

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
                                try:
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
                                except:
                                    print("Skipping!")    
                    start_date += delta
                    if len(self.compaign_data) > 0:
                        self.insert_compaign(self.compaign_data, self.siteid)
                except KeyError as e:
                        print(f"KeyError occurred: {e}")   
                        break  
                except Exception as e:
                    print("An error occurred:", str(e))
                    break
            
        except Exception as e:
            print("An error occurred:", str(e))
        
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
            
    def insert_compaign(self, data, id):
        print('insert data to pre stage')
        try:
            total = len(data)
            count = 0
            failed = 0
            table = 'pages'
            self.connection_db()
           
            query = f"INSERT INTO pre_stage.traffic_channels_v3 (siteid, server_date, firsturl, referrer_type, referrer_name, referrer_keyword, date, hour, batch_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)"
            
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


    def truncate(self,site):
        try:
            self.connection_db()
            print('start turncate for siteid : ',site)
            truncate_query = f"DELETE FROM pre_stage.traffic_channels_v3 WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            truncate_query = f"DELETE FROM stage.traffic_channels_v3 WHERE siteid = {site};"
            self.cursor.execute(truncate_query)
            self.connection.commit()
            self.cursor.close()
            self.connection.close()
        except Exception as error:
            print(f"Error: {error}")
            
    def insert_data_into_stage(self,siteid,domain):
        print('Data insertion start into Stage')
        try:
            query = f"""INSERT INTO stage.traffic_channels_v3 (server_date,postid, siteid, date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, batch_id)
                        SELECT a.server_date,b.ID AS postid, a.siteid, a.date, a.hour, a.firsturl, a.referrer_type, a.referrer_name , a.referrer_keyword, a.batch_id
                        FROM pre_stage.traffic_channels_v3  a
                        LEFT JOIN prod.site_archive_post b ON a.firsturl = b.Link
                        WHERE  a.siteid = {siteid};
            """
            update_link_query14 =f"""
                UPDATE pre_stage.traffic_channels_v3 a
                        SET a.firsturl = 
                            CASE
                                WHEN a.firsturl REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.firsturl, '?', 1)
                                ELSE a.firsturl
                            END
                        WHERE a.SITEID = {siteid}
            """
            print('insert_data_into_stage for site 13')

            query1 = f"""
                UPDATE stage.traffic_channels_v3
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
            try:
                self.connection_db() 
                self.cursor.execute(update_link_query14)
                self.cursor.execute(query)
                self.connection.commit()
                self.cursor.execute(query1)
                self.connection.commit()
                print("Insertion complete into Stage")
            except Exception as error:
                print(f"Error: {error}")

            self.connection.close()   
        except Exception as e:
            print("An error occurred:", str(e))
        
        #  this is start function ,that call all function of this class
    def insert_data_into_prod(self,siteid):
        print('start insertion into Production')

        print('delete entry pages for site')
        query_delete = f"""delete  from prod.entry_pages_agg WHERE Siteid = {siteid} and date = '{self.yesterday}'     """
        print('insert_data_into_stage for site 13')
        query = f"""INSERT INTO prod.entry_pages_agg (Siteid, Date, RealReferrer, referrer_name, referrer_keyword, firsturl, hits)
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
                                END AS referrer_keyword, firsturl
                                FROM stage.traffic_channels_v3
                                    WHERE Siteid = {siteid}
                            )
                        SELECT siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl,  COUNT(*) as Hits
                        FROM conditions 
                        GROUP BY siteid, date,RealReferrer, referrer_name, referrer_keyword, firsturl;
            """
        
        print('delete data for site 13')
        query1_delete = f"""delete  from prod.visits_rythm WHERE Siteid = {siteid} and date = '{self.yesterday}' """
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
                                    FROM stage.traffic_channels_v3
                                        WHERE Siteid ={siteid}
                                )
                            SELECT Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday, COUNT(*) as Hits
                            FROM conditions 
                            GROUP BY Siteid, Date, Hour, RealReferrer, referrer_name, referrer_keyword, Weekday;
        """

        print(f'delete real referrer for site {siteid}')
        query2_delete = f"""delete  from prod.realreferrer WHERE Siteid = {siteid} and date = '{self.yesterday}'"""

        query2 = f"""INSERT INTO  prod.realreferrer (Siteid, Date, RealReferrer, Hits)
                    SELECT Siteid, Date, RealReferrer, COUNT(*) as hits
                    FROM   stage.traffic_channels_v3 where siteid = {siteid}
                    GROUP BY Siteid, Date, RealReferrer;
        """

        query3 = f"""
            INSERT INTO prod.traffic_channels (SiteID, postid , date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, RealReferrer, batch_id )
            SELECT SiteID, postid , date, hour, firsturl, referrer_type, referrer_name, referrer_keyword, RealReferrer, batch_id FROM stage.traffic_channels_v3
            where siteid = {siteid};
        """

        query3_delete = f""" delete  from prod.traffic_channels  where siteid = {siteid} and date = '{self.yesterday}';"""

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
            print('Completed insertion!')
        except Exception as error:
            print(f"Error: {error}")
        self.connection.close()

    def truncate_data_quality(self, site):

        self.connection_db()
        cursor = self.cursor
        connection = self.connection

        current_date = (datetime.now()).strftime('%Y-%m-%d')
        print(current_date)

        sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND swagger_status IS NULL AND traffic_status IS NULL AND analytics_status IS NULL AND cms_status IS NULL and siteid = {site}"""
        
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
            INSERT INTO prod.posting_status (siteid, posting_date, traffic_ingestion_status)
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

    def start_execution(self):
        for id in [13]:
            try: 
                print('Siteid : ',id)
                self.truncate(id)
                self.truncate_data_quality(id)
                self.connection_db()

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
                self.send_data_quality(13, True)
                self.connection_db()
            except mysql.connector.Error as error:
                self.send_data_quality(13, False)
                print("Error inserting data into MySQL database:", error)

# create a object of class
def dev_etl():
    etl = Etl()
    logging.info(etl.matomo_url)
    etl.start_execution()