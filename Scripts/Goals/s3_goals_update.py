import mysql.connector
import pandas as pd
import json
import datetime
import boto3
import re
import time
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import warnings
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)

warnings.filterwarnings('ignore')

class goals_ETL:
    def __init__(self):
        # keys to access the AWS buckets
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")

        # the source bucket name
        self.bucket_name = "dashboard-goals"
        # store the goals that are in the files
        self.goals = []
        # store the names of the files
        self.names_of_files = []
        self.file_verf = None
        self.site_id = None
        self.date_str = None
        self.data_df = pd.DataFrame()
        self.failed_etl = []
        self.etl_log_goals = []
        self.max_goals_id = None
        self.max_batch_id = None
        self.schema = None
        self.sites = []
        # prod database credential
        self.cursor = None
        self.connection = None
        self.host = os.getenv("DB_HOST")
        self.port = int(os.getenv("DB_PORT"))
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")

    def s3_client(self):
        
        self.truncate_stage()

        print('Starting the Goals ETL')
        
        # create a connection to the S3 bucket
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url="https://s3.nl-ams.scw.cloud",
        )
        
        # get all the objects that are inside the bucket
        response = s3.list_objects_v2(Bucket=self.bucket_name)

        print("Bucket Details: " + str(response))

        # iterate over the objects that are in the bucket
        for index, obj in enumerate(response.get("Contents", [])):
            # get the name of the object
            file_key = obj["Key"]
            local_file_name = f"file{index}.json"
            # append it to the overall object list of our S3
            file_name = str(file_key)
            print(file_name)
            self.names_of_files.append(file_name)
        # exit()
        try:
            # print how many objects do we currenly have in the S3 bucket
            print("len of file",len(self.names_of_files))
            if len(self.names_of_files) != 0:

                # convert the names into a dataframe
                df = pd.DataFrame({
                    "Filename": self.names_of_files
                })

                # Extract the Site ID and Date from the Filename using regexp
                df[["Site ID", "Date"]] = df["Filename"].str.extract(r"site-(\d+)-user-\d+-goals-(\d{4}-\d{2}-\d{2})T")

                # Convert the Date column to datetime type
                df["Date"] = pd.to_datetime(df["Date"])

                # Sort the DataFrame by Site ID, Date, and Time
                df_sorted = df.sort_values(by=["Site ID", "Date", "Filename"], ascending=[True, False, True])

                file_name_list = df_sorted['Filename'].tolist()

                # Find the index of the maximum row for each Site ID
                max_index = df_sorted.groupby("Site ID")["Date"].idxmax()

                # Get the corresponding file names for the maximum rows
                max_files = df_sorted.loc[max_index, "Filename"].tolist()

                """In our max_index object, we now have the latest goals against each sites"""

                # now lets update the goals for each site according to the latest goal files in our S3 bucket
                for i in max_files:
                    # clean up the stage and pre_stage tables
                    print("Truncating Pre Stage")
                    self.truncate_prestage()

                    file_key = i
                     # Extract the Site ID  
                    self.site_id = re.search(r"site-(\d+)", file_key).group(1)

                    print('Updating goals for site id: ',self.site_id)

                    # Extract the date
                    self.date_str = re.search(r"(\d{4}-\d{2}-\d{2})", file_key).group(1)
                    print('Goals date: ',self.date_str)

                    self.sites.append(self.site_id)

                    # Read the file contents
                    try:
                        # request the object details from the S3 bucket
                        response = s3.get_object(Bucket=self.bucket_name, Key=file_key)
                        # get the json_data inside the file that represents the goals
                        file_content = response["Body"].read().decode("utf-8")
                        json_data = json.loads(file_content)
                        print(json_data)

                        # Start date (Jan 1st of the current year)
                        start_date = datetime.date(datetime.datetime.now().year, 1, 1)

                        print("Inserting Dates Into Pre Stage")
                        self.connection_db()
                        # Loop through each day of the year and insert it into the stage database
                        for i in range(366):  # Adjust for leap years if needed
                            current_date = start_date + datetime.timedelta(days=i)
                            
                            # Get the week number and month number
                            week_number = current_date.isocalendar()[1]  # ISO week number
                            month_number = current_date.month  # Month as a number
                            
                            query = """
                            INSERT INTO pre_stage.goals(Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day, Min_pageviews, Min_CTA, Site_ID, batch_id)
                            VALUES (%s, %s, %s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                            """
                            
                            # Execute the query with the current date, week number, and month number
                            self.cursor.execute(query, (current_date, week_number, month_number))
                            self.connection.commit()

                        # get the current year
                        current_year  = datetime.datetime.now().year
                        year_c = str(current_year)
                        print("Updating Goals In Stage, For The Year: " + str(year_c))

                        print("Updating the first quater ...")
                        #  For the first quater (q1)
                        d1 = year_c + '-01-01'
                        d2 = year_c + '-03-31'
                        # Assign the values to the DataFrame
                        try: 
                            if int(json_data['q11']) >= 0:
                                va = json_data['q11']
                                print("Updating CTA_per_day")
                                query = f"UPDATE pre_stage.goals SET CTA_per_day = '{va}', Quarter = 'Q1', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q21']) >= 0:
                                va = json_data['q21']
                                print("Updating Pageviews_per_day")
                                query = f"UPDATE pre_stage.goals SET Pageviews_per_day = '{va}', Quarter = 'Q1', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q31']) >= 0:

                                va = json_data['q31']
                                print("Updating Visits_per_day")
                                query = f"UPDATE pre_stage.goals SET Visits_per_day = '{va}', Quarter = 'Q1', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))

                        #  q2                        
                        print("Updating the second quater ...")
                        d1 = year_c + '-04-01'
                        d2 = year_c + '-06-30'
                        try:
                            if int(json_data['q12']) >= 0:
                                va = json_data['q12']
                                print("Updating CTA_per_day")
                                query = f"UPDATE pre_stage.goals SET CTA_per_day = '{va}', Quarter = 'Q2', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q22']) >= 0:
                                va = json_data['q22']
                                print("Updating Pageviews_per_day")
                                query = f"UPDATE pre_stage.goals SET Pageviews_per_day = '{va}', Quarter = 'Q2', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q32']) >= 0:  
                                # df.loc[df['Quarter'] == 'Q2', 'Visits_per_day'] = json_data['q32']
                                va = json_data['q32']
                                print("Updating Visits_per_day")
                                query = f"UPDATE pre_stage.goals SET Visits_per_day = '{va}', Quarter = 'Q2', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))

                        #  q3
                        print("Updating the third quater ...")
                        d1 = year_c + '-07-01'
                        d2 = year_c + '-09-30'
                        try:
                            if int(json_data['q13']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q3', 'CTA_per_day'] = json_data['q13']
                                va = json_data['q13']
                                print("Updating CTA_per_day")
                                query = f"UPDATE pre_stage.goals SET CTA_per_day = '{va}', Quarter = 'Q3', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q23']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q3', 'Pageviews_per_day'] = json_data['q23']
                                va = json_data['q23']
                                print("Updating Pageviews_per_day")
                                query = f"UPDATE pre_stage.goals SET Pageviews_per_day = '{va}', Quarter = 'Q3', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q33']) >= 0:  
                                # df.loc[df['Quarter'] == 'Q3', 'Visits_per_day'] = json_data['q33']
                                va = json_data['q33']
                                print("Updating Visits_per_day")
                                query = f"UPDATE pre_stage.goals SET Visits_per_day = '{va}', Quarter = 'Q3', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))

                        #  q4
                        print("Updating the fourth quater ...")
                        d1 = year_c + '-10-01'
                        d2 = year_c + '-12-31'
                        try:
                            if int(json_data['q14']) >= 0:
                                # df.loc[df['Quarter'] == 'Q4', 'CTA_per_day'] = json_data['q14']
                                va = json_data['q14']
                                print("Updating CTA_per_day")
                                query = f"UPDATE pre_stage.goals SET CTA_per_day = '{va}', Quarter = 'Q4', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q24']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q4', 'Pageviews_per_day'] = json_data['q24']
                                va = json_data['q24']
                                print("Updating Pageviews_per_day")
                                query = f"UPDATE pre_stage.goals SET Pageviews_per_day = '{va}', Quarter = 'Q4', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q34']) >= 0:
                                # df.loc[df['Quarter'] == 'Q4', 'Visits_per_day'] = json_data['q34']
                                va = json_data['q34']
                                print("Updating Visits_per_day")
                                query = f"UPDATE pre_stage.goals SET Visits_per_day = '{va}', Quarter = 'Q4', Site_ID = {self.site_id} WHERE date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        
                        print("Updating Pageviews and CTA ...")
                        d1 = year_c + '-01-01'
                        d2 = year_c + '-12-31'
                        try:
                            if int(json_data['q41']) >= 0:
                                va = json_data['q41']
                                query = f"UPDATE pre_stage.goals SET Min_pageviews = '{va}' WHERE site_id = {self.site_id} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q51']) >= 0:
                                va = json_data['q51']
                                query = f"UPDATE pre_stage.goals SET Min_CTA = '{va}' WHERE site_id = {self.site_id} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        self.cursor.close()
                        self.connection.close()
                        print('ending')
                        site_id_value = int(self.site_id)
                        # data post
                        self.insert_into_stage(site_id_value)
                        print(f"Complete insertion in stage for site: {site_id_value}")
                    except Exception as e:
                        print("Error:", str(e))
                self.buckets_file_archive(file_name_list)
            else:
                print("There are no files in the S3 bucket")
        except Exception as e:
            self.connection.close()
            print("No new file is available on s3 --> (Error) :", str(e))
            
    #  this function create the connection of database
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

            print(' Database Connection successfull')
            
        except mysql.connector.Error as error:
            print("Database Connection Error:", error)

    def insert_into_stage(self, site):
        print(f"Starting Insertion Into Stage For Site: {site}")
        query = f"""
            INSERT INTO stage.goals(Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id)
            select Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id from pre_stage.goals
            where Site_ID = {site};
        """
        self.connection_db()
        self.cursor.execute(query)
        self.connection.commit()
        print(f"Finished Insertion Into Stage For Site: {site}")

    def insert_into_prod(self):
        # print how many objects do we currenly have in the S3 bucket
        print("Started Insertion of goals into Prod")

        print("len of file",len(self.names_of_files))
        if len(self.names_of_files) != 0:
            # now lets update the goals for each site according to the latest goal files in our S3 bucket
            for i in self.sites:

                 # Extract the Site ID  
                self.site_id = i
                print('Updating goals for site id: ',self.site_id)

                # get the current year
                current_year  = datetime.datetime.now().year
                year_c = str(current_year)

                print(f"Delete Goals For Site: {self.site_id} and Year: {year_c}")
                # delete the goals for the current year from prod database
                delete_query = f"""
                    DELETE FROM prod.goals WHERE Site_ID = {self.site_id} and Year(Date) = {year_c}
                """

                # insert the new goals into the prod database
                print(f"Insert Goals For Site: {self.site_id} and Year: {year_c}")
                query = f"""
                    INSERT INTO prod.goals(Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id)
                    select Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id from stage.goals
                    WHERE Site_ID = {self.site_id};
                """
                self.connection_db()
                self.cursor.execute(delete_query)
                self.connection.commit()
                self.cursor.execute(query)
                self.connection.commit()
            
            
            
    def start_execution_goals(self):
        self.batch_insert()
        self.s3_client()
        print(self.sites)
        self.insert_into_prod()
            
    # its create and add the batch details 
    def batch_insert(self):
        print('batch_insert')
        try:
            self.connection_db()
            query = "INSERT INTO stage.batch_name (Type, status) VALUES (%s, %s)"
            values = ('Goals', 'initialize')
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
            self.cursor.close()
            self.connection.close()

        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
            self.cursor.close()
            self.connection.close()


    def truncate_prestage(self):
        print('start')
        try:
            self.connection_db()
            query1 = """
            TRUNCATE TABLE pre_stage.goals;
            """
            self.cursor.execute(query1)
            time.sleep(3)
                    # close the database connection  
            self.cursor.close()
            self.connection.close()
            print('Pre Stage table truncated!!')
        except mysql.connector.Error as error:
            print(error)
            self.cursor.close()
            self.connection.close()

    def truncate_stage(self):
        print('start')
        try:
            self.connection_db()
            query1 = """
            TRUNCATE TABLE stage.goals;
            """
            self.cursor.execute(query1)
            time.sleep(3)
                    # close the database connection  
            self.cursor.close()
            self.connection.close()
            print('Stage table truncated!!')
        except mysql.connector.Error as error:
            print(error)
            self.cursor.close()
            self.connection.close()

    def buckets_file_archive(self,file_name_list):
        try:
            # Specify the source and destination bucket names
            source_bucket_name = "dashboard-goals"
            destination_bucket_name = "archive-dashboard-goals"

            # Create an S3 client with the specified endpoint URL
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url="https://s3.nl-ams.scw.cloud",
            )

            # Create the new bucket
            # s3.create_bucket(Bucket=destination_bucket_name)

            # List objects in the source bucket
            response = s3.list_objects_v2(Bucket=source_bucket_name)

            # Move objects from the source bucket to the destination bucket
            for obj in response['Contents']:
                # Get the object key
                object_key = obj['Key']

                if object_key in file_name_list:
                    # Copy the object to the destination bucket
                    s3.copy_object(
                        CopySource={'Bucket': source_bucket_name, 'Key': object_key},
                        Bucket=destination_bucket_name,
                        Key=object_key
                    )
                    # Delete the object from the source bucket
                    s3.delete_object(Bucket=source_bucket_name, Key=object_key)
            print("Files moved to the new bucket successfully!")
        except mysql.connector.Error as error:
            print("File not move to archive bucket:", error)

    def restore_file_to_source(self):
        try:
            # Specify the source and destination bucket names
            source_bucket_name = "dashboard-goals"
            destination_bucket_name = "archive-dashboard-goals"
            object_key = "site-18-user-86-goals-2025-03-27T09:41:55.json"

            # Create an S3 client
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url="https://s3.nl-ams.scw.cloud",
            )

            # Check if the file exists in the destination bucket
            response = s3.list_objects_v2(Bucket=destination_bucket_name)
            
            print(response)
            # exit()

            if 'Contents' not in response:
                print(f"File '{object_key}' not found in the destination bucket.")
                return

            # Copy the object from the destination bucket to the source bucket
            s3.copy_object(
                CopySource={'Bucket': destination_bucket_name, 'Key': object_key},
                Bucket=source_bucket_name,
                Key=object_key
            )

            print(f"File '{object_key}' restored to the source bucket successfully!")

        except Exception as error:
            print("Error restoring file to source bucket:", error)

def dev_etl():
    etl = goals_ETL()
    etl.start_execution_goals()

    # etl.restore_file_to_source()

dev_etl()