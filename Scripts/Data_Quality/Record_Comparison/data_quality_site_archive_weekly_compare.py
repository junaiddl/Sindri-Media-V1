import mysql.connector
import logging
from datetime import datetime, timedelta
from mysql.connector import Error as MySQLError
import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from contextlib import redirect_stdout
import smtplib
from data_quality_site_archive_weekly_compare_4 import insert_into_prod as prod4
from data_quality_site_archive_weekly_compare_10 import insert_into_prod as prod10
from data_quality_site_archive_weekly_compare_11 import insert_into_prod as prod11
from data_quality_site_archive_weekly_compare_13 import insert_into_prod as prod13
from data_quality_site_archive_weekly_compare_14 import insert_into_prod as prod14
from data_quality_site_archive_weekly_compare_15 import insert_into_prod as prod15
from data_quality_site_archive_weekly_compare_16 import insert_into_prod as prod16
from data_quality_site_archive_weekly_compare_17 import insert_into_prod as prod17
from data_quality_site_archive_weekly_compare_18 import insert_into_prod as prod18
from data_quality_site_archive_weekly_compare_19 import insert_into_prod as prod19
from analytics_etl_v2 import Etl
from analytics_etl_v2 import dev_etl_analytics



sites = [4, 10, 11, 13, 14, 15, 16, 17, 18, 19]

comparison_data = []
missing_data = []

yesterday = datetime.today().date() - timedelta(days=7)
yesterday_str = yesterday.strftime('%Y-%m-%d')

fetching_end = datetime.today().date() - timedelta(days=1)
fetching_end_str = fetching_end.strftime('%Y-%m-%d')

today = datetime.today().date()
today_str = today.strftime('%Y-%m-%d')


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
        <h1>Swagger ETL Weekly Comparison</h1>
        <p>Dear Team,</p>
        <p>I trust this email finds you well.</p>
        <p>Kindly review the Swagger ETL report for the weekly records of the past 7 days to find any issues with the records in the Database & the records sent via the API!</p>
    """

def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
              host = os.getenv("DB_HOST"),
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASSWORD"),
            port = int(os.getenv("DB_PORT")),
        )
        print('Database Connected...')
        return connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)

def compare_data(siteid):
    """
    Compares the count of records in two tables.
    """
    try:
        connection = get_database_connection()
        cursor = connection.cursor()


        # SQL queries to count records in the two tables
        query_pre_stage = """
            SELECT COUNT(*) FROM pre_stage.site_archive_post_v2 WHERE siteid = %s AND date >= %s AND date <= %s;
        """
        query_prod = """
            SELECT COUNT(*) from pre_stage.site_archive_post_v2 pre
            LEFT JOIN prod.site_archive_post pro
            ON pre.id = pro.id AND pre.siteid = pro.siteid
            WHERE pre.siteid = %s AND pre.date >= %s AND pre.date <= %s
            AND pro.id IS NOT NULL
            ORDER BY pre.date;
        """

        missing_data_query = """
            SELECT pre.id, pre.siteid, pre.Title, pre.Date, pre.Link from pre_stage.site_archive_post_v2 pre
            LEFT JOIN prod.site_archive_post pro
            ON pre.id = pro.id AND pre.siteid = pro.siteid
            WHERE pre.siteid = %s AND pre.date >= %s AND pre.date <= %s
            AND pro.id IS NULL
            ORDER BY pre.date;
        """

        missing_data_in_prod = """
            SELECT pre.id, pre.siteid, pre.Title, pre.Date, pre.Link from pre_stage.site_archive_post_v2 pre
            LEFT JOIN prod.site_archive_post pro
            ON pre.id = pro.id AND pre.siteid = pro.siteid 
            WHERE pre.siteid = %s AND pre.date >= %s AND pre.date <= %s
            AND pro.id IS NULL
            ORDER BY pre.date;
        """

        unmatched_url_query = """
            SELECT pre.id, pre.siteid, pre.Title, pre.Date, pre.Link AS pre_url, pro.Link AS prod_url
            FROM pre_stage.site_archive_post_v2 pre
            JOIN prod.site_archive_post pro
            ON pre.id = pro.id AND pre.siteid = pro.siteid
            WHERE pre.siteid = %s 
            AND pre.date BETWEEN %s AND %s
            AND pre.Link <> pro.Link
            ORDER BY pre.date;
        """
        
        # Execute queries
        cursor.execute(query_pre_stage, (siteid, yesterday, fetching_end_str))
        pre_stage_count = cursor.fetchone()[0]

        cursor.execute(query_prod, (siteid, yesterday, fetching_end_str))
        prod_count = cursor.fetchone()[0]

        cursor.execute(missing_data_query, (siteid, yesterday, fetching_end_str))
        miss_data = cursor.fetchall()

        cursor.execute(missing_data_in_prod, (siteid, yesterday, fetching_end_str))
        miss_data_prod = cursor.fetchall()

        cursor.execute(unmatched_url_query, (siteid, yesterday, fetching_end_str))
        unmatched_url = cursor.fetchall()



        print(miss_data)

        data = [siteid, pre_stage_count, prod_count]

        # Log the comparison results
        logging.info("Comparison Results for Site ID %s:", siteid)
        logging.info("Pre-Stage Record Count: %s", pre_stage_count)
        logging.info("Prod Record Count Since %s: %s", yesterday, prod_count)

        # Log the comparison results
        print(f"Comparison Results for Site ID {siteid}:")
        print(f"Pre-Stage Record Count: {pre_stage_count}")
        print(f"Prod Record Count Since {yesterday}: {prod_count}")

        return data, miss_data, miss_data_prod, unmatched_url
    except MySQLError as e:
        logging.error("Error comparing data: %s", e)
        raise
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to send an email with CSV attachments
def mailConnection(comparison_data, missing_data):
    sender_email = 'alert@sindri.media'
    sender_password = 'password'
    # receiver_emails = ['syed.ashhar@dotlabs.ai', 'junaid@dotlabs.ai']
    receiver_emails = ['syed.ashhar@dotlabs.ai', 'muhammad.ali@dotlabs.ai']

    # Create the email content
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_emails)
    msg['Subject'] = 'Data Quality Check: ETL Weekly Comparison Status'

    # Construct the HTML content for the email
    html_content = header

    html_content += "<table border='1' style='border-collapse: collapse; width: 100%;'>"
    html_content += "<tr style='background-color: #f2f2f2;'><th>Site ID</th><th>API Records</th><th>Database Records</th><th>Status</th></tr>"

    for data in comparison_data:
        html_content += f"<tr><td>{data[0]}</td><td>{data[1]}</td><td>{data[2]}</td>"
        if(data[1] == data[2]):
            html_content += f"<td>✓</td></tr>"
        else:
            html_content += f"<td>𐄂</td></tr>"

    html_content += "</table>"

    for siteid, results, prod_data_miss, different_urls in missing_data:
        if results:
            # Add site header
            html_content += f"<h2>Missing Data for Site ID {siteid}</h2>"
            # Start table
            html_content += """
            <table border="1" style="border-collapse: collapse; width: 100%;">
                <tr>
                    <th>Post ID</th>
                    <th>Site ID</th>
                    <th>Title</th>
                    <th>Date</th>
                    <th>Link</th>
                </tr>
            """
            # Add rows
            for row in results:
                post_id, site_id, title, date, link = row
                html_content += f"""
                <tr>
                    <td>{post_id}</td>
                    <td>{site_id}</td>
                    <td>{title}</td>
                    <td>{date}</td>
                    <td><a href="{link}">{link}</a></td>
                </tr>
                """
            # End table
            html_content += "</table><br>"
        else:
            html_content += f"<p>No missing data for Site ID {siteid}.</p>"

        # PROD_MISS
        if prod_data_miss:
            # Add site header
            html_content += f"<h2>Missing Prod Data for Site ID {siteid}</h2>"
            # Start table
            html_content += """
            <table border="1" style="border-collapse: collapse; width: 100%;">
                <tr>
                    <th>Post ID</th>
                    <th>Site ID</th>
                    <th>Title</th>
                    <th>Date</th>
                    <th>Link</th>
                </tr>
            """
            # Add rows
            for row in prod_data_miss:
                post_id, site_id, title, date, link = row
                html_content += f"""
                <tr>
                    <td>{post_id}</td>
                    <td>{site_id}</td>
                    <td>{title}</td>
                    <td>{date}</td>
                    <td><a href="{link}">{link}</a></td>
                </tr>
                """
            # End table
            html_content += "</table><br>"
        else:
            html_content += f"<p>No missing prod data for Site ID {siteid}.</p>"

        # URL_DIFF
        if different_urls:
            # Add site header
            html_content += f"<h2>Different URLs data for Site ID {siteid}</h2>"
            # Start table
            html_content += """
            <table border="1" style="border-collapse: collapse; width: 100%;">
                <tr>
                    <th>Post ID</th>
                    <th>Site ID</th>
                    <th>Title</th>
                    <th>Date</th>
                    <th>Pre_Link</th>
                    <th>Prod_Link</th>
                </tr>
            """
            # Add rows
            for row in different_urls:
                post_id, site_id, title, date, pre_link, prod_link = row
                html_content += f"""
                <tr>
                    <td>{post_id}</td>
                    <td>{site_id}</td>
                    <td>{title}</td>
                    <td>{date}</td>
                    <td><a href="{pre_link}">{pre_link}</a></td>
                    <td><a href="{prod_link}">{prod_link}</a></td>
                </tr>
                """
            # End table
            html_content += "</table><br>"
        else:
            html_content += f"<p>No different URLs data for Site ID {siteid}.</p>"


    html_content += "</body></html>"

    msg.attach(MIMEText(html_content, 'html'))

    # Send the email
    smtp_server = 'send.one.com'
    smtp_port = 465
    with open('nul', 'w') as null_file, redirect_stdout(null_file):
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_emails, msg.as_string())

    print('Email sent successfully!')

def add_postid(siteid, postid, link):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        normalized_link = link.rstrip('/')

        query_newsletter = f'''
            UPDATE prod.newsletter SET post_id = {postid} WHERE site_id = {siteid} AND TRIM(TRAILING '/' FROM url) = '{normalized_link}';
        '''
        query_events = f'''
            UPDATE prod.events SET postid = {postid} WHERE siteid = {siteid} AND TRIM(TRAILING '/' FROM event_name) = '{normalized_link}';
        '''
        query_traffic_channels = f'''
            UPDATE prod.traffic_channels SET postid = {postid} WHERE siteid = {siteid} AND TRIM(TRAILING '/' FROM firsturl) = '{normalized_link}';
        '''

        print("UPDATING THE POSTID USING THE FOLLOWING QUERIES:")
        print(query_newsletter)
        print(query_events)
        print(query_traffic_channels)
       
        # cursor.execute(query_newsletter)
        # cursor.execute(query_events)
        # cursor.execute(query_traffic_channels)
        # connection.commit()
    except Exception as e:
        print("Error:   ", e)


def insert_missing_posts(missing_data):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        devetl_called = {
            4: False,
            10: False,
            11: False,
            13: False,
            14: False,
            15: False,
            16: False,
            17: False,
            18: False,
            19: False
        }
        for siteid, site_miss, _, _ in missing_data:
            for row in site_miss:
                post_id, site_id, title, date, link = row
                globals()[f"prod{site_id}"](site_id, post_id, link, date, cursor, connection)
                add_postid(site_id, post_id, link)
                if devetl_called[siteid] != True:
                    dev_etl_analytics(siteid)
                    devetl_called[siteid] = True
                etl_class = Etl()
                etl_class.insert_data_into_prod(connection, cursor, siteid = site_id, postid = post_id)

        # connection.commit()
        print("Missing posts inserted successfully into prod.site_archive_post.")

    except MySQLError as e:
        logging.error("Error inserting missing posts: %s", e)
        raise
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def updateLinks(missing_data):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        for siteid, _, _, diff_urls in missing_data:
            for row in diff_urls:
                print(row)
                post_id, site_id, title, date, newlink, oldlink = row
                normalized_oldlink = oldlink.rstrip('/')
                normalized_newlink = newlink.rstrip('/')

                update_prod = f"""UPDATE prod.site_archive_post SET link = '{normalized_newlink}' 
                WHERE siteid = {site_id} AND date = '{date}' AND id = {post_id} AND link = '{normalized_oldlink}';"""

                update_prod_slash = f"""UPDATE prod.site_archive_post SET link = '{normalized_newlink + '/'}' 
                WHERE siteid = {site_id} AND date = '{date}' AND id = {post_id} AND link = '{normalized_oldlink + '/'}';"""

                update_events = f"""UPDATE prod.events SET event_name = '{normalized_newlink}'
                WHERE siteid = {site_id} AND postid = {post_id} AND event_name = '{normalized_oldlink}';"""

                update_events_slash = f"""UPDATE prod.events SET event_name = '{normalized_newlink + '/'}'
                WHERE siteid = {site_id} AND postid = {post_id} AND event_name = '{normalized_oldlink + '/'}';"""

                update_pages = f"""UPDATE prod.pages SET URL = '{normalized_newlink}'
                WHERE siteid = {site_id} AND postid = {post_id} AND URL = '{normalized_oldlink}';"""

                update_pages_slash = f"""UPDATE prod.pages SET URL = '{normalized_newlink + '/'}'
                WHERE siteid = {site_id} AND postid = {post_id} AND URL = '{normalized_oldlink + '/'}';"""

                update_newsletter = f"""UPDATE prod.newsletter SET URL = '{normalized_newlink}'
                WHERE site_id = {site_id} AND post_id = {post_id} AND URL = '{normalized_oldlink}';"""

                update_newsletter_slash = f"""UPDATE prod.newsletter SET URL = '{normalized_newlink + '/'}'
                WHERE site_id = {site_id} AND post_id = {post_id} AND URL = '{normalized_oldlink + '/'}';"""

                update_traffic = f"""UPDATE prod.traffic_channels SET firsturl = '{normalized_newlink}'
                WHERE siteid = {site_id} AND postid = {post_id} AND firsturl = '{normalized_oldlink}';"""

                update_traffic_slash = f"""UPDATE prod.traffic_channels SET firsturl = '{normalized_newlink + '/'}'
                WHERE siteid = {site_id} AND postid = {post_id} AND firsturl = '{normalized_oldlink + '/'}';"""

                print(update_prod)
                print(update_prod_slash)
                print(update_events)
                print(update_events_slash)
                print(update_pages)
                print(update_pages_slash)
                print(update_newsletter)
                print(update_newsletter_slash)
                print(update_traffic)
                print(update_traffic_slash)

        print("URLs updated successfully into prod.site_archive_post.")

    except MySQLError as e:
        logging.error("Error updating URLs: %s", e)
        raise
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def start_execution():
    for siteid in sites:
        site_data, site_miss, prod_data_miss, diff_urls = compare_data(siteid)
        print(site_data)
        print(site_miss)
        comparison_data.append(site_data)
        missing_data.append([siteid, site_miss, prod_data_miss, diff_urls])
    
    print(comparison_data)
    print(missing_data)

    
    mailConnection(comparison_data, missing_data)

    if missing_data:
        insert_missing_posts(missing_data)
        updateLinks(missing_data)


# Call the main function to start your program
def dev_etl():
    logging.info("Data Quality API & DB Records Comparison")
    start_execution()




if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()
