import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

# Load data from an Excel file
file_path = '/home/ashhar/Desktop/Sindri_Media/Scripts/missing_userneeds.csv'
df = pd.read_csv(file_path, delimiter=';')


def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cursor = connection.cursor()
        print('Database connection successful')
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None


def generate_sql_queries(df):
    queries = []
    for index, row in df.iterrows():
        site_id = row['Site_ID']
        post_id = row['ID']
        user_needs = row['Userneeds']
        date = row['Date']

        query = f"INSERT INTO pre_stage.userneeds (id,siteid, prediction, date) VALUES ({post_id},{site_id},'{user_needs}','{date}')"
        queries.append(query)
    return queries

def insert_into_prestage():
    # Generate SQL queries
    sql_queries = generate_sql_queries(df)
    print(sql_queries)
    # exit()
    cursor, connection = get_database_connection()

    for query in sql_queries:
        print(query)
        cursor.execute(query)

    connection.commit()

    query1= f"update pre_stage.userneeds set prediction ='Hjælp mig med at forstå' where prediction ='Help mig med at forsta'"
    cursor.execute(query1)
    connection.commit()

def insert_into_prod():
    try:
        cursor, connection = get_database_connection()
        query =f"""UPDATE prod.site_archive_post e 
                    JOIN pre_stage.userneeds s ON e.id = s.id  and e.siteid = s.siteid
                    SET e.userneeds = s.prediction;"""
        result = cursor.execute(query)
        connection.commit()
        print(result)
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)



def start_execution():
    print("Started Insertion Into Stage")
    insert_into_prestage()
    print("Started Insertion Into Prod")
    insert_into_prod()

start_execution()