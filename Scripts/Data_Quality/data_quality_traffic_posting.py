import os
import mysql.connector
import smtplib
from contextlib import redirect_stdout
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

traffic_failure_sites = []

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
        <h1>Traffic Channel Data Posting Issues</h1>
        <p>Dear Team,</p>
        <p>I trust this email finds you well.</p>
        <p>There was an issue in the traffic channel posting of one or more sites. Kindly look at the table below for more details!</p>
    </body>
    </html>"""

def get_database_connection():
    connection = mysql.connector.connect(
       host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
    )
    return connection.cursor(), connection

def mailConnection():
    sender_email = 'alert@sindri.media'
    sender_password = 'password'
    receiver_emails = ['syed.ashhar@dotlabs.ai']
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = 'Data Quality Check: Issue In Traffic Channel Data Posting'
    msg.attach(MIMEText(header, 'html'))

    email_body = ""
    if traffic_failure_sites:
        email_body += """
        <h3>Traffic Channel Issues:</h3>
        <table>
            <tr>
                <th>Site ID</th>
                <th>Posting Date</th>
                <th>Traffic Posting Status</th>
                <th>Traffic Ingestion Status</th>
            </tr>"""
        for site in traffic_failure_sites:
            traffic_status = "Success" if site[2] == 1 else "Failed"
            ingestion_status = "Success" if site[3] == 1 else "Failed"
            email_body += f"""
            <tr>
                <td>{site[0]}</td>
                <td>{site[1]}</td>
                <td>{traffic_status}</td>
                <td>{ingestion_status}</td>
            </tr>"""
        email_body += """
        </table>
        <p>Best Regards,</p>
        <p>DotLabs Dev Team</p>"""

    msg.attach(MIMEText(email_body, 'html'))
    smtp_server = 'send.one.com'
    smtp_port = 465

    with open('nul', 'w') as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print('Email sent successfully!')

def fetch_dq_details(site):
    cursor, connection = get_database_connection()

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    print(current_date)

    sql_query =f"""SELECT 
                        siteid, 
                        posting_date, 
                        MAX(COALESCE(traffic_status, 0)) AS traffic_status, 
                        MAX(COALESCE(traffic_ingestion_status, 0)) AS traffic_ingestion_status 
                    FROM prod.posting_status
                    WHERE siteid = {site} AND posting_date = '{current_date}'
                    AND cms_status IS NULL AND analytics_status IS NULL AND swagger_status IS NULL
                    GROUP BY siteid, posting_date
                """
    
    cursor.execute(sql_query)
    result = cursor.fetchall()
    print(result)

    if not result:
        traffic_failure_sites.append([site, current_date, 0, 0])
    else:
        for row in result:
            siteid, posting_date, traffic, ingestion = row
            if traffic == 0 or ingestion == 0:
                traffic_failure_sites.append([siteid, posting_date, traffic, ingestion])

def quality_start():
    sendMail = False
    sites = [4, 11, 13, 14, 15, 16, 17]
    
    for siteid in sites:
        print(f"Checking Data Posting For Site: {siteid}")
        fetch_dq_details(siteid)

    print("Traffic Posting Status")
    print(traffic_failure_sites, len(traffic_failure_sites))

    for status in traffic_failure_sites:
        if status[2] == 0 or status[3] == 0:
            print("An error in posting was found!")
            sendMail = True

    if sendMail:
        mailConnection()
        
if __name__ == "__main__":
    quality_start()