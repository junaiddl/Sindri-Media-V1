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
from email import encoders

# List of site IDs to process
sites = [4, 10, 11, 13, 14, 15, 16, 17]

# HTML header for the email content
header = """<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>ETL Status</title></head><body><h1>ETL Status</h1>"""

# Function to send an email with CSV attachments
def mailConnection(mail_data, file_paths):

    print("Starting To Send Mail")

    sender_email = 'alert@sindri.media'
    sender_password = 'password'
    receiver_emails = ['syed.ashhar@dotlabs.ai']


    # Create the email content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = 'Data Quality Check: ETL Status'

    # Construct the HTML content for the email
    html_content = header

    # Add tables for each category of data
    for category in ['CMS', 'Traffic Channels', 'Newsletter', 'Pages', 'Events', 'Daily Totals']:
        html_content += f"<h2>{category} Data</h2>"
        html_content += "<table border='1' style='border-collapse: collapse; width: 100%;'>"
        html_content += "<tr style='background-color: #f2f2f2;'><th>Site ID</th><th>Details</th></tr>"

        for site_id, counts in mail_data[category].items():
            html_content += f"<tr><td>{site_id}</td>"
            details = []
            if category == 'Traffic Channels' or category == 'CMS' or category == 'Newsletter':
                details.append(f"Pre Stage: {counts.get('pre_count', 'N/A')}")
                details.append(f"Stage: {counts.get('stage_count', 'N/A')}")
                details.append(f"Prod: {counts.get('prod_count', 'N/A')}")
            elif category == 'Pages':
                details.append(f"Prod: {counts.get('prod_count', 'N/A')}")
            elif category == 'Events':
                details.append(f"Prod: {counts.get('prod_count', 'N/A')}")
            elif category == 'Daily Totals':
                details.append(f"Prod: {counts.get('prod_count', 'N/A')}")
                
            html_content += f"<td>{', '.join(details)}</td>"            
            html_content += "</tr>"

        html_content += "</table>"

    html_content += "</body></html>"  # Close the HTML body

    msg.attach(MIMEText(html_content, 'html'))

    # Attach CSV files
    for file_path in file_paths:
        with open(file_path, 'rb') as file:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            msg.attach(part)

    # Send the email
    smtp_server = 'send.one.com'
    smtp_port = 465
    with open('nul', 'w') as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print('Email sent successfully!')

# Function to get the database connection
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
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)

# Function to run the ETL process and generate the report
def run_etl_and_generate_report():
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    connection = get_database_connection()
    cursor = connection.cursor()

    email_data = {
        'CMS': {},
        'Traffic Channels': {},
        'Newsletter': {},
        'Pages': {},
        'Events': {},
        'Daily Totals': {}
    }
    
    # Prepare lists to hold data for CSV files
    site_archive_post_data = []
    traffic_channels_data = []
    newsletter_data = []
    pages_data = []
    events_data = []
    daily_totals_data = []
    
    csv_files = []  # List to store CSV file paths

    try:
        # Process Traffic Channels Data
        for site in sites:
            # Traffic Channels Queries (Pre Stage, Stage, Prod)
            cursor.execute(f"SELECT COUNT(*) FROM pre_stage.site_archive_post_v2 WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_pre_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM stage.site_archive_post_v2 WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM prod.site_archive_post WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()

            # Fetch site_archive_post data
            sql = f"""SELECT id, title, link, date FROM pre_stage.site_archive_post_v2 WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_pre_stage = cursor.fetchall()
            
            sql = f"""SELECT id, title, link, date FROM stage.site_archive_post_v2 WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_stage = cursor.fetchall()
            
            sql = f"""SELECT id, title, link, date FROM prod.site_archive_post WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for site_archive_post
            for article in article_detail_pre_stage:
                site_archive_post_data.append([site] + list(article) + ['Pre Stage'])
            for article in article_detail_stage:
                site_archive_post_data.append([site] + list(article) + ['Stage'])
            for article in article_detail_prod:
                site_archive_post_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['CMS'][site] = {
                'pre_count': articles_in_pre_stage_yesterday[0],
                'stage_count': articles_in_stage_yesterday[0],
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Process Traffic Channels Data
        for site in sites:
            cursor.execute(f"SELECT COUNT(*) FROM pre_stage.traffic_channels_v3 WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_pre_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM stage.traffic_channels_v3 WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM prod.traffic_channels WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()

            # Fetch traffic_channels data
            sql = f"""SELECT Date, referrer_type, referrer_name, referrer_keyword, firsturl, postid, RealReferrer FROM pre_stage.traffic_channels_v3 WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_pre_stage = cursor.fetchall()
            
            sql = f"""SELECT Date, referrer_type, referrer_name, referrer_keyword, firsturl, postid, RealReferrer FROM stage.traffic_channels_v3 WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_stage = cursor.fetchall()
            
            sql = f"""SELECT Date, referrer_type, referrer_name, referrer_keyword, firsturl, postid, RealReferrer FROM prod.traffic_channels WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for traffic_channels
            for article in article_detail_pre_stage:
                traffic_channels_data.append([site] + list(article) + ['Pre Stage'])
            for article in article_detail_stage:
                traffic_channels_data.append([site] + list(article) + ['Stage'])
            for article in article_detail_prod:
                traffic_channels_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['Traffic Channels'][site] = {
                'pre_count': articles_in_pre_stage_yesterday[0],
                'stage_count': articles_in_stage_yesterday[0],
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Process Pages Data
        for site in sites:
            cursor.execute(f"SELECT COUNT(*) FROM prod.pages WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()
            
            sql = f"""SELECT postid, date, unique_pageviews, URL FROM prod.pages WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for analytics
            for article in article_detail_prod:
                pages_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['Pages'][site] = {
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Process Events Data
        for site in sites:
            cursor.execute(f"SELECT COUNT(*) FROM prod.events WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()
            
            sql = f"""SELECT postid, date, event_category, event_action, event_name, hits FROM prod.events WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for analytics
            for article in article_detail_prod:
                events_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['Events'][site] = {
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Process Daily Totals Data
        for site in sites:
            cursor.execute(f"SELECT COUNT(*) FROM prod.daily_totals WHERE siteid = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()
            
            sql = f"""SELECT id, Date, Visits, Unique_pageviews FROM prod.daily_totals WHERE siteid = %s AND date = '{yesterday_str}' ORDER BY id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for analytics
            for article in article_detail_prod:
                daily_totals_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['Daily Totals'][site] = {
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Process Traffic Channels Data
        for site in sites:
            cursor.execute(f"SELECT COUNT(*) FROM pre_stage.newsletter WHERE site_id = {site} AND date = '{yesterday_str}'")
            articles_in_pre_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM stage.newsletter WHERE site_id = {site} AND date = '{yesterday_str}'")
            articles_in_stage_yesterday = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM prod.newsletter WHERE site_id = {site} AND date = '{yesterday_str}'")
            articles_in_prod_yesterday = cursor.fetchone()

            # Fetch traffic_channels data
            sql = f"""SELECT Date, referrer_type, referrer_name, url, post_id, visits FROM pre_stage.newsletter WHERE site_id = %s AND date = '{yesterday_str}' ORDER BY post_id"""
            cursor.execute(sql, (site,))
            article_detail_pre_stage = cursor.fetchall()
            
            sql = f"""SELECT Date, referrer_type, referrer_name, url, post_id, visits FROM stage.newsletter WHERE site_id = %s AND date = '{yesterday_str}' ORDER BY post_id"""
            cursor.execute(sql, (site,))
            article_detail_stage = cursor.fetchall()
            
            sql = f"""SELECT Date, referrer_type, referrer_name, url, post_id, visits FROM prod.newsletter WHERE site_id = %s AND date = '{yesterday_str}' ORDER BY post_id"""
            cursor.execute(sql, (site,))
            article_detail_prod = cursor.fetchall()

            # Collect data for traffic_channels
            for article in article_detail_pre_stage:
                newsletter_data.append([site] + list(article) + ['Pre Stage'])
            for article in article_detail_stage:
                newsletter_data.append([site] + list(article) + ['Stage'])
            for article in article_detail_prod:
                newsletter_data.append([site] + list(article) + ['Prod'])

            # Example structure for email_data
            email_data['Newsletter'][site] = {
                'pre_count': articles_in_pre_stage_yesterday[0],
                'stage_count': articles_in_stage_yesterday[0],
                'prod_count': articles_in_prod_yesterday[0],
            }

        # Convert the lists to DataFrames
        df_site_archive_post = pd.DataFrame(site_archive_post_data, columns=['siteid', 'id', 'title', 'link', 'date', 'Stage'])
        # df_traffic_channels = pd.DataFrame(traffic_channels_data, columns=['siteid', 'Date', 'referrer_type', 'referrer_name', 'referrer_keyword', 'firsturl', 'postid', 'RealReferrer', 'Stage'])
        # df_pages = pd.DataFrame(pages_data, columns=['siteid', 'postid', 'date', 'unique_pageviews', 'URL', 'Stage'])
        # df_events = pd.DataFrame(events_data, columns=['siteid', 'postid', 'date', 'event_category', 'event_action', 'event_name', 'hits', 'Stage'])
        # df_daily_totals = pd.DataFrame(daily_totals_data, columns=['siteid', 'id', 'Date', 'Visits', 'Unique_pageviews', 'Stage'])


        # Define CSV file paths
        file_path_site_archive_post = 'site_archive_post.csv'
        # file_path_traffic_channels = 'traffic_channels.csv'
        # file_path_pages = 'pages.csv'
        # file_path_events = 'events.csv'
        # file_path_daily_totals = 'daily_totals.csv'

        # Save DataFrames to CSV files
        df_site_archive_post.to_csv(file_path_site_archive_post, index=False)
        # df_traffic_channels.to_csv(file_path_traffic_channels, index=False)
        # df_pages.to_csv(file_path_pages, index=False)
        # df_events.to_csv(file_path_events, index=False)
        # df_daily_totals.to_csv(file_path_daily_totals, index=False)


        # csv_files.extend([file_path_site_archive_post, file_path_traffic_channels, file_path_pages, file_path_events, file_path_daily_totals])

        csv_files.extend([file_path_site_archive_post])


        # Send email with CSV files attached
        mailConnection(email_data, csv_files)

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
    finally:
        cursor.close()
        connection.close()

run_etl_and_generate_report()