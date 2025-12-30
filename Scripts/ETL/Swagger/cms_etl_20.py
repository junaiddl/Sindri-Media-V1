import time
import itertools
import requests
import mysql.connector
from datetime import datetime
from calendar import monthrange
import logging
import sys,os
config_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(1, config_path)
from config_file import host, port, user, password
def get_database_connection():
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port,
    )

def truncate(siteid):
    try:
        conn = get_database_connection()
        cur = conn.cursor()
        cur.execute(f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[TRUNCATE ERROR] {e}")

def insert_data(data_list):
    try:
        conn = get_database_connection()
        cur = conn.cursor()
        insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 
            (id, Title, Date, Link, Tags, siteid)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cur.executemany(insert_query, data_list)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[INSERT ERROR] {e}")


# def parse_date_smart(day_month_str, current_page, previous_month, previous_year):
#     try:
#         day, month = map(int, day_month_str.strip().split("/"))
#         year = previous_year

#         # Handle year transition if month jumps backward in page order
#         if current_page > 1 and month > previous_month:
#             year -= 1

#         parsed = datetime(year, month, day)
#         return parsed.strftime("%Y-%m-%d"), month, year
#     except:
#         return None, previous_month, previous_year

def parse_date_smart(day_month_str, previous_month, previous_year):
    try:
        day, month = map(int, day_month_str.strip().split("/"))
        year = previous_year

        # Only decrement year if it wraps around from Jan to Dec
        if previous_month == 1 and month == 12:
            year -= 1

        parsed = datetime(year, month, day)
        return parsed.strftime("%Y-%m-%d"), month, year
    except Exception as e:
        print(f"[DATE PARSE ERROR] {e} | Raw: {day_month_str}")
        return None, previous_month, previous_year



    
def get_latest_postid(siteid):
    try:
        conn = get_database_connection()
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(id) FROM prod.site_archive_post WHERE siteid = {siteid}")
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result if result else 0
    except Exception as e:
        print(f"[MAX ID ERROR] {e}")
        return 0

def normalize_link(link):
    if link:
        if not link.startswith("http"):
            link = f"https://fagbladetboligen.dk{link}"
        link = link.rstrip("/")  # Remove trailing slash
    return link



def article_exists(title, link, siteid):
    try:
        link = normalize_link(link)  # Normalize before checking
        conn = get_database_connection()
        cur = conn.cursor()
        query = """
            SELECT COUNT(*) FROM prod.site_archive_post
            WHERE Link = %s AND Title = %s AND siteid = %s
        """
        cur.execute(query, (link, title, siteid))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"[CHECK EXIST ERROR] {e}")
        return False


def fetch_articles(api_url, siteid, starting_postid):
    page = 1
    postid = starting_postid + 1
    all_data = []
    spinner = itertools.cycle(['-', '\\', '|', '/'])

    today = datetime.today()
    previous_month = today.month
    previous_year = today.year

    while True:
        spin = next(spinner)
        print(f"{spin} Extracting page: {page}", end='\r')
        url = f"{api_url}&page={page}"
        try:
            res = requests.get(url)
            res.raise_for_status()
            json_data = res.json()
            articles = json_data.get("pages", [])
            load_more = json_data.get("loadMore", False)

            found_existing = False

            for article in articles:
                title = article.get("Title")
                raw_date = article.get("DateOfPublish")
                date, previous_month, previous_year = parse_date_smart(raw_date, previous_month, previous_year)
                link = normalize_link(article.get("Url"))  # Normalize here
                tag = article.get("Tag", {}).get("Name", None)

                if article_exists(title, link, siteid):
                    found_existing = True
                    break

                all_data.append((postid, title, date, link, tag, siteid))
                postid += 1

            if found_existing or not load_more:
                break
            page += 1

        except Exception as e:
            print(f"[FETCH ERROR] {e}")
            break

    return all_data




def insert_into_stage():
    connection = get_database_connection()
    print("got connection")
    cursor = connection.cursor()
    print("got cursor")
    insert_query = '''
        INSERT INTO stage.site_archive_post_v2 (id, Title, Date, Link, Categories, Tags, Status, Modified, siteid)
        SELECT
            s.id,
            s.Title,
            s.Date,
            s.Link,
            s.Categories,
            s.Tags,
            s.Status,
            s.Modified,
            s.siteid
        FROM pre_stage.site_archive_post_v2 s
        WHERE s.siteid = 20;'''
    
    url_update_query = '''
        UPDATE stage.site_archive_post_v2
        SET Link = LEFT(Link, LENGTH(Link) - 1)
        WHERE siteid = 20 and RIGHT(Link, 1) = '/';
        '''
    tags_update_query = '''
        UPDATE prod.site_archive_post
        SET tags = REPLACE(tags, '&amp;', '&')
        WHERE siteid = 20;
        '''
    try:
        cursor.execute(insert_query)
        connection.commit()
        print("ran insert query")
        logging.info('Data insertion successful into stage')

        cursor.execute(url_update_query)
        connection.commit()
        print("ran url_update query")
        logging.info('url updated successfully in stage')

        cursor.execute(tags_update_query)
        connection.commit()
        print("ran tags_update query")
        logging.info('tags updated successfully in stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting or updating data into stage MySQL database: %s", e)
        connection.rollback()
        raise


def insert_into_prod(siteid):
    connection = get_database_connection()
    cursor = connection.cursor()
    start_date = datetime.today().strftime('%Y-%m-%d')
    
    try:

        insert_into_historic = f"""
        INSERT INTO prod.site_archive_post_historic (id, Title, Date, Link, Categories, Tags, Status, Modified, siteid, inserted_at)
            SELECT d.id, d.Title, d.Date, d.Link, d.Categories, d.Tags,  d.Status, d.Modified, d.siteid, '{start_date}'
                FROM stage.site_archive_post_v2 s
                LEFT JOIN prod.site_archive_post d
				    ON s.siteid = d.siteid AND s.ID = d.id 
                        WHERE d.id IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
                        AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                            ON DUPLICATE KEY UPDATE
                                Title = VALUES(Title),
                                Date = VALUES(Date),
                                Link = VALUES(Link),
                                Categories = VALUES(Categories),
                                Tags = VALUES(Tags),
                                Status = VALUES(Status),
                                Modified = VALUES(Modified),
                                siteid = VALUES(siteid),
                                inserted_at = VALUES(inserted_at);
        """

        """AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);"""

        insert_link_updates_query = f"""
                INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
                    SELECT s.id AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
                        FROM stage.site_archive_post_v2 s
                        LEFT JOIN prod.site_archive_post_historic d
                            ON s.siteid = d.siteid AND s.id = d.ID AND s.Link != d.Link
                                WHERE s.siteid = {siteid} and d.LINK IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        delete_old_records = f"""
            WITH IDS AS (
            SELECT d.ID
                FROM prod.site_archive_post d
                LEFT JOIN stage.site_archive_post_v2 s
                    ON s.siteid = d.siteid AND s.id = d.ID
                WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            )
            DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID from IDS) AND d.siteid = {siteid};
        """

        insert_query = f"""
            INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, siteid)
                SELECT s.ID, s.Title, s.Date, s.Link, s.categories, s.Tags, s.Status, s.Modified, s.siteid
                    FROM stage.site_archive_post_v2 s
                    LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid and s.id = d.ID 
                        WHERE d.ID IS NULL AND d.Date is null And s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """

        update_events = f"""
            UPDATE prod.events e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.event_name = lu.old_link
                AND lu.postid = e.postid
            SET e.event_name = lu.new_link
            WHERE e.siteid = lu.siteid AND e.event_name = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        update_pages = f"""
            UPDATE prod.pages e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.URL = lu.old_link
                AND lu.postid = e.postid
            SET e.URL = lu.new_link
            WHERE e.siteid = lu.siteid AND e.URL = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        update_traffic_channel = f"""
            UPDATE prod.traffic_channels e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.firsturl = lu.old_link
                AND lu.postid = e.postid
            SET e.firsturl = lu.new_link
            WHERE e.siteid = lu.siteid AND e.firsturl = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """

        cursor.execute(insert_into_historic)
        cursor.execute(insert_link_updates_query)
        cursor.execute(delete_old_records)
        cursor.execute(insert_query)
        cursor.execute(update_events)
        cursor.execute(update_pages)
        cursor.execute(update_traffic_channel)
        connection.commit()

        print('Data Inserted Into Prod')
    except Exception as e:
        print(e)

def start_execution():
    siteid = 20
    api_url = "https://fagbladetboligen.dk/ws/newslibrarypage/articlesasjson?route=ivA0JCiPrpTtVt_1rr1lPy72XaOzEjpfoFOqIgiMjGk"

    print(f"Truncating existing data for site {siteid}")
    truncate(siteid)

    print("Checking latest postid in DB...")
    latest_postid = get_latest_postid(siteid)
    print(f"Latest postid in database is:   {latest_postid}")

    print("Fetching articles...")
    articles = fetch_articles(api_url, siteid, latest_postid)

    print(f"Inserting {len(articles)} articles into pre_stage...")
    insert_data(articles)

    if not articles:
        print("No new articles fetched. Skipping stage and prod inserts.")
        return

    insert_into_stage()
    insert_into_prod(siteid)

    print("✅ ETL complete.")


def dev_etl():
    start_execution()

if __name__ == "__main__":
    dev_etl()