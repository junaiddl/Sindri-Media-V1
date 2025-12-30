import os
import mysql.connector
import logging
import datetime
import pandas as pd
import smtplib
from contextlib import redirect_stdout
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

sites = [4, 10, 11, 13, 14, 15, 16, 17]

header = """ <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f8f9fa;
                color: #343a40;
            }
            h1 {
                color: #00796b;
                font-size: 1.5rem;
            }
            p {
                font-size: 1rem;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }
            table, th, td {
                border: 1px solid #ddd;
            }
            th, td {
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #00796b;
                color: white;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <h1>Traffic ETL Status</h1>
        <p>Dear Team,</p>
        <p>I trust this email finds you well.</p>
        <p>Kindly take a look to verify the Traffic Channel Data Ingestion for the current sites!</p>
    """

def mailConnection(mail_data):

    # Email configuration
    sender_email = 'alert@sindri.media'
    sender_password = 'RnmAmcLoSHq5oLcnoa'

    receiver_emails = ['syed.ashhar@dotlabs.ai', 'junaid@dotlabs.ai']


    # Create the email content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = 'Data Quality Check: Traffic ETL Status'
    
    html_content = header
    
    for site_id, details in mail_data.items():
        # Site ID and Counts
        html_content += f"<h2>Site ID: {site_id}</h2>"
        html_content += f"<p><strong>Pre Stage Count:</strong> {details['pre_count']}</p>"
        html_content += f"<p><strong>Stage Count:</strong> {details['stage_count']}</p>"
        html_content += f"<p><strong>Prod Count:</strong> {details['prod_count']}</p>"

    # Attach the constructed email body to the message
    msg.attach(MIMEText(html_content, 'html'))

    # Connect to the SMTP server
    smtp_server = 'send.one.com'
    smtp_port = 465

    # Redirect the standard output to suppress the SMTP conversation output
    with open('nul', 'w') as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print('Email sent successfully!')

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
        logging.info('successfull')
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)

def check_data_quality():

    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)

    yesterday_str = yesterday.strftime('%Y-%m-%d')

    connection = get_database_connection()
    cursor = connection.cursor()

    email_data = {}

    try:
        for site in sites:
            print("\n\n")
            print(f"Site: {site}", end="\n\n")
            try:
                sql = f"""SELECT COUNT(*) FROM pre_stage.traffic_channels_v3 WHERE siteid = %s AND date = '{yesterday_str}'"""
                cursor.execute(sql, (site,))  # Execute the query with parameter
                articles_in_pre_stage_yesterday = cursor.fetchone()  # Fetch the result
                print(f"Number of Articles In Pre_Stage: {articles_in_pre_stage_yesterday[0]}")
            except Exception as e:
                print(e)
            try:
                sql = f"""SELECT COUNT(*) FROM stage.traffic_channels_v3 WHERE siteid = %s AND date = '{yesterday_str}'"""
                cursor.execute(sql, (site,))  # Execute the query with parameter
                articles_in_stage_yesterday = cursor.fetchone()  # Fetch the result
                print(f"Number of Articles In Stage: {articles_in_stage_yesterday[0]}")
            except Exception as e:
                print(e)

            try:
                sql = f"""SELECT COUNT(*) FROM prod.traffic_channels WHERE siteid = %s AND date = '{yesterday_str}'"""
                cursor.execute(sql, (site,))  # Execute the query with parameter
                articles_in_prod_yesterday = cursor.fetchone()  # Fetch the result
                print(f"Number of Articles In Prod: {articles_in_prod_yesterday[0]}")
            except Exception as e:
                print(e)

            email_data[site] = {
                'pre_count' : articles_in_pre_stage_yesterday[0],
                'stage_count' : articles_in_stage_yesterday[0],
                'prod_count' : articles_in_prod_yesterday[0],
            }
            print("\n\n")
        mailConnection(email_data)
    except Exception as e:
        print(e)