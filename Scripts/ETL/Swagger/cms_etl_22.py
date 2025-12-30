import logging
import mysql.connector
import mysql
import requests
import json
from urllib.parse import urljoin
from datetime import datetime, timedelta
import os
import sys
config_path = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(1, config_path)
from config_file import host, port, user, password


def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host = host,
            port = port,
            user = user,
            password = password
        )

        return connection
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)

BASE_URL = "https://taenk.dk/api/v1/teasers"  
API_KEY  = "api key"         


# Dates
yesterday = datetime.today().date() - timedelta(days=1)
today = datetime.today().date()

# Convert to strings for later use
from_date = yesterday.strftime("%Y-%m-%d")
to_date = today.strftime("%Y-%m-%d")

params = {
    "from": from_date,
    "to":   to_date
   
}

headers = {
    "api-key": API_KEY,
    "Accept": "application/json"
}
def fetch_all_teasers(base_url, headers, params):
    """Fetch all pages, following 'next' until exhausted."""
    results = []
    next_url = base_url
    next_params = dict(params) if params else {}

    while next_url:
        resp = requests.get(next_url, headers=headers, params=next_params if next_url == base_url else None)
        if resp.status_code != 200:
            raise RuntimeError(f"Request failed: {resp.status_code} {resp.text}")

        data = resp.json()
        results.extend(data.get("results", []))

        nxt = data.get("next")
        if not nxt:
            break
        next_url = nxt if nxt.startswith(("http://", "https://")) else urljoin(base_url, nxt)
        next_params = None  # after first page, rely on 'next' URL only

    return results


def names_list(items):
    """Return comma-separated 'name' values from list of {uuid, name} objects."""
    return ", ".join((itm.get("name") or "").strip() for itm in (items or []) if isinstance(itm, dict))


def extract_url_tags(url):
    """Custom tags derived from URL patterns."""
    mapping = {
        "/forbrugerliv": "forbrugerliv",
        "/test": "test",
        "/kemi": "kemi",
        "/privatoekonomi": "privatoekonomi",  # fixed spelling!
        "/raadgivning": "raadgivning"
    }
    tags = [tag for key, tag in mapping.items() if key in (url or "")]
    return ", ".join(tags)


def truncate():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_query1 = f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = 22;"
        cursor.execute(truncate_query1)
        connection.commit()
        print("Truncated pre_stage.site_archive_post_v2 successfully.")
    except Exception as e:
        print(e)




def insert_into_pre_stage(cursor, connection, data_list):
    print("Starting To Insert Data Into Pre_Stage")
    for data in data_list:
        print(data)

    insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (uuid,Title,Date,Modified,Link,Categories,Tags,tags_r,siteid,created)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s,%s)
            """
    try:
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info('Data insertion successful in pre_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into pre_stage MySQL database: %s", e)
        connection.rollback()
        raise



def insert_into_prod_stage():
    connection = get_database_connection()
    cursor = connection.cursor()
    insert_query = """
  INSERT INTO prod.site_archive_post_string (
    id,
    siteid,
    Title,
    Date,
    Link,
    Categories,
    Tags,
    created
)
SELECT
    s.uuid AS id,
    s.siteid,
    s.Title,
    s.Date,
    s.Link,
    s.Categories,
    s.tags_r AS Tags,
    s.created
FROM pre_stage.site_archive_post_v2 s
WHERE s.siteid = 22
ON DUPLICATE KEY UPDATE
    siteid = VALUES(siteid),
    Title = VALUES(Title),
    Date = VALUES(Date),
    Link = VALUES(Link),
    Categories = VALUES(Categories),
    Tags = VALUES(Tags),
    created = VALUES(created);
        """
    try:
        cursor.execute(insert_query)
        connection.commit()
        logging.info('Data insertion successful in prod_stage')
    except mysql.connector.Error as e:
        logging.error("Error inserting data into prod_stage MySQL database: %s", e)
        connection.rollback()
        raise


def print_article_teaser():
    connection = get_database_connection()
    cursor = connection.cursor()
    try:
        all_teasers = fetch_all_teasers(BASE_URL, headers, params)
    except Exception as e:
        print("❌ Error fetching teasers:", e)
        return
    """Print only the required fields from CMS API."""
    extracted_data = []
    for t in all_teasers:
       
        uuid_      = t.get("uuid")
        title      = t.get("title")
        date_      = t.get("date")      # publication date
        updated    = t.get("updated")   # modified date
        url        = t.get("url")
        categories = names_list(t.get("categories", []))  # joined names
        subjects   = names_list(t.get("subjects", []))   # joined names
        url_tags   = extract_url_tags(url)
        siteid     = 22
        created    = t.get("created")
        # Append as tuple
        extracted_data.append(
            (uuid_, title, date_, updated, url, categories, subjects, url_tags, siteid,created)
        )
    insert_into_pre_stage(cursor, connection, extracted_data)
    

def start_execution():
        truncate()
        print_article_teaser()
        insert_into_prod_stage()

def dev_etl():
    logging.info("Site 22 Archive Post")
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()