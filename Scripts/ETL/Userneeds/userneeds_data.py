# -*- coding: utf-8 -*-

import os
import requests
import mysql.connector
import json
import logging


def get_database_connection():
    try:
        connection = mysql.connector.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
            charset='utf8mb4',  # Specify UTF-8 encoding
        )
        cursor = connection.cursor()
        return cursor, connection

    except mysql.connector.Error as error:
        print("Error connecting to the MySQL database:", error)
 
  
def execute_queries(cursor, connection, queries):
    try:
        for query in queries:
            cursor.execute(query)
        # Commit the transaction
        connection.commit()
        print("Queries executed successfully!")

    except mysql.connector.Error as error:
        print("Error executing queries:", error)
        # Rollback in case of an error
        connection.rollback()

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()


def dev_etl():
    # Establish the database connection
    cursor, connection = get_database_connection()

    # Define the queries to be executed
    queries = [
        "update `userneeds-data`.user_needs set prediction='Hjælp mig med at forstå' where prediction ='Help mig med at forsta';",
        "update prod.site_archive_post p join `userneeds-data`.user_needs c on p.siteid =c.client and p.id=c.articleid set userneeds=prediction;"
        
    ]
    # Execute the queries
    execute_queries(cursor, connection, queries)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()