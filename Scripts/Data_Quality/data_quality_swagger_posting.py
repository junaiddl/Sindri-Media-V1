import mysql.connector
import smtplib
from contextlib import redirect_stdout
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os


failure_sites = []

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
        <h1>Swagger Data Posting Issues</h1>
        <p>Dear Team,</p>
        <p>I trust this email finds you well.</p>
        <p>There was an issue in the swagger data posting of one or more sites. Kindly look at the table below for details.</p>
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
    msg['Subject'] = 'Data Quality Check: Issue In Swagger Data Posting'
    msg.attach(MIMEText(header, 'html'))

    email_body = ""
    if failure_sites:
        email_body += """
        <h3>Swagger Data Posting Issues:</h3>
        <table>
            <tr>
                <th>Site ID</th>
                <th>Posting Date</th>
                <th>Swagger Status</th>
                <th>CMS Ingestion Status</th>
                <th>Analytics Ingestion Status</th>
            </tr>"""
        for site in failure_sites:
            swagger_status = "Success" if site[2] == 1 else "Failed"
            cms_status = "Success" if site[3] == 1 else "Failed"
            analytics_status = "Success" if site[4] == 1 else "Failed"
            email_body += f"""
            <tr>
                <td>{site[0]}</td>
                <td>{site[1]}</td>
                <td>{swagger_status}</td>
                <td>{cms_status}</td>
                <td>{analytics_status}</td>
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
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    sql_query = f"""SELECT 
                        siteid, 
                        posting_date, 
                        MAX(COALESCE(swagger_status, 0)) AS swagger_status, 
                        MAX(COALESCE(cms_status, 0)) AS cms_status, 
                        MAX(COALESCE(analytics_status, 0)) AS analytics_status
                    FROM prod.posting_status
                    WHERE siteid = {site} AND posting_date = '{current_date}'
                    AND traffic_status IS NULL AND traffic_ingestion_status IS NULL
                    GROUP BY siteid, posting_date
                """
    
    cursor.execute(sql_query)
    result = cursor.fetchall()

    if not result:
        failure_sites.append([site, current_date, 0, 0, 0])
    else:
        for row in result:
            siteid, posting_date, swagger, cms, analytics = row
            if swagger == 0 or cms == 0 or analytics == 0:
                failure_sites.append([siteid, posting_date, swagger, cms, analytics])

def quality_start():
    sendMail = False
    sites = [4, 10, 11, 13, 14, 15, 16, 17]
    
    for siteid in sites:
        print(f"Checking Data Posting For Site: {siteid}")
        fetch_dq_details(siteid)

    print("Data Posting Status")
    print(failure_sites, len(failure_sites))

    for status in failure_sites:
        if status[2] == 0 or status[3] == 0 or status[4] == 0:
            print("An error in posting was found!")
            sendMail = True

    if sendMail:
        mailConnection()

if __name__ == "__main__":
    quality_start()
