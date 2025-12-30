import mysql.connector
import smtplib
import csv
import os
from contextlib import redirect_stdout
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta

missing_userneeds_map = {}

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
        <h1>Missing Userneeds Summary</h1>
        <p>Dear Team,</p>
        <p>I trust this email finds you well.</p>
        <p>Kindly take a look to verify that if any userneeds are missing for the current sites!</p>
        <table>
        <tr>
            <th>Site ID</th>
            <th>ID</th>
            <th>Title</th>
            <th>Link</th>
            <th>Date</th>
        </tr>
    """

def get_database_connection():
    # Connect to the MySQL database
    connection = mysql.connector.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
    )
    cursor = connection.cursor()
    return cursor, connection

def mailConnection():
    # Email configuration
    sender_email = 'alert@sindri.media'
    sender_password = 'password'

    # receiver_emails = ['junaid@dotlabs.ai', 'nouman@dotlabs.ai', 'kasper@sindriconsult.dk','syed.ashhar@dotlabs.ai']
    receiver_emails = ['syed.ashhar@dotlabs.ai']


    # Create the email content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = 'Data Quality Check: Missing Userneeds'
    
    email_body = header

    for siteid, userneeds in missing_userneeds_map.items():
        if userneeds:
            email_body += f"<tr><td colspan='5' style='background-color:#e0e0e0;'><strong>Site ID: {siteid}</strong></td></tr>"
            for entry in userneeds:
                email_body += f"<tr><td>{siteid}</td><td>{entry[0]}</td><td>{entry[1]}</td><td><a href='{entry[2]}'>{entry[2]}</a></td><td>{entry[3]}</td></tr>"
    
    email_body += """
        </table>
        <p>Best Regards,</p>
        <p>Dot Labs Dev Team</p>
    </body>
    </html>"""

    # Attach the constructed email body to the message
    msg.attach(MIMEText(email_body, 'html'))

    # Attach the CSV file
    csv_filename = 'missing_userneeds.csv'
    with open(csv_filename, 'rb') as csvfile:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csvfile.read())
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename={csv_filename}',
        )
        msg.attach(part)

    # Connect to the SMTP server
    smtp_server = 'send.one.com'
    smtp_port = 465

    # Redirect the standard output to suppress the SMTP conversation output
    with open('nul', 'w') as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print('Email sent successfully!')

def generate_csv():
    csv_filename = 'missing_userneeds.csv'
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['Site_ID', 'ID', 'Title', 'Link', 'Date', 'Userneeds']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for siteid, userneeds in missing_userneeds_map.items():
            for entry in userneeds:
                writer.writerow({
                    'Site_ID': siteid,
                    'ID': entry[0],
                    'Title': entry[1],
                    'Link': entry[2],
                    'Date': entry[3],
                    'Userneeds':''
                })
    
    print('CSV file generated successfully!')

def fetch_dq_details(site):
    cursor, connection = get_database_connection()

    sql_query = f"""select id, title, link, date from 
                    prod.site_archive_post where userneeds is null 
                    and Date > DATE_SUB(CURDATE(), INTERVAL 8 DAY) and siteid = {site}"""
    
    cursor.execute(sql_query)
    result = cursor.fetchall()
    print(result)

    missing_userneeds_map[site] = result

def quality_start():
    sendMail = False
    sites = [4,10,11,13,14,15,16,17,18]
    for siteid in sites:
        print(f"Checking Missing Userneeds For Site: {siteid}")
        fetch_dq_details(siteid)

    print(missing_userneeds_map)
    
    # Generate the CSV file
    generate_csv()

    for site, userneeds in missing_userneeds_map.items():
        if(len(userneeds) != 0):
            sendMail = True
            print("Found Missing Userneeds!")

    if(sendMail):
        # Send the email with the CSV attachment
        mailConnection()

    # Remove the CSV file from the local system
    os.remove('missing_userneeds.csv')
    print('CSV file removed successfully!')

if __name__ == "__main__":
    quality_start()
