import mysql.connector
import os
from dotenv import load_dotenv

# Provide full path to the .env file
dotenv_path = r"C:\Users\Admin\Desktop\Sindri_Media\.env"
load_dotenv(dotenv_path=dotenv_path)
# Establish a connection to the MySQL database
def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = connection.cursor()
        return cursor, connection
    except mysql.connector.Error as error:
        print("Error connecting to MySQL database:", error)
        return None, None

data_dict = {
    53: 'overview_sales_card_dbt_',
    54: 'overview_pageviews_card_dbt_',
    55: 'overview_visits_card_dbt_',
    56: 'overview_sales_chart_day_dbt_',
    57: 'overview_sales_chart_week_dbt_',
    58: 'overview_sales_chart_month_dbt_',
    59: 'overview_pageviews_chart_day_dbt_',
    60: 'overview_pageviews_chart_week_dbt_',
    61: 'overview_pageviews_chart_month_dbt_',
    62: 'overview_visits_chart_day_dbt_',
    63: 'overview_visits_chart_week_dbt_',
    64: 'overview_visits_chart_month_dbt_',
    65: 'tactical_articles_card_month_dbt_',
    66: 'tactical_articles_card_ytd_dbt_',
    67: 'tactical_clicks_months_dbt_',
    68: 'tactical_clicks_ytd_dbt_',
    69: 'tactical_pageviews_card_month_dbt_',
    70: 'tactical_pageviews_card_ytd_dbt_',
    71: 'tactical_userneeds_clicks_chart_month_dbt_',
    72: 'tactical_userneeds_clicks_chart_ytd_dbt_',
    73: 'tactical_userneeds_pageviews_chart_month_dbt_',
    74: 'tactical_userneeds_pageviews_chart_ytd_dbt_',
    75: 'tactical_category_clicks_chart_month_dbt_',
    76: 'tactical_category_clicks_chart_ytd_dbt_',
    77: 'tactical_category_pageviews_chart_month_dbt_',
    78: 'tactical_category_pageviews_chart_ytd_dbt_',
    79: 'tactical_articles_table_month_dbt_',
    80: 'tactical_articles_table_ytd_dbt_',
    81: 'performance_articles2_card_day_dbt_',
    82: 'performance_articles2_card_month_dbt_',
    83: 'performance_articles2_card_ytd_dbt_',
    84: 'performance_articles1_card_day_dbt_',
    85: 'performance_articles1_card_month_dbt_',
    86: 'performance_articles1_card_ytd_dbt_',
    87: 'performance_articles0_card_day_dbt_',
    88: 'performance_articles0_card_month_dbt_',
    89: 'performance_articles0_card_ytd_dbt_',
    90: 'performance_articles0_tables_day_dbt_',
    91: 'performance_articles0_tables_month_dbt_',
    92: 'performance_articles0_tables_ytd_dbt_',
    93: 'performance_articles1_tables_day_dbt_',
    94: 'performance_articles1_tables_month_dbt_',
    95: 'performance_articles1_tables_ytd_dbt_',
    96: 'performance_articles2_tables_day_dbt_',
    97: 'performance_articles2_tables_month_dbt_',
    98: 'performance_articles2_tables_ytd_dbt_',
    99: 'set_article_dbt_',
    100: 'set_sitestats_dbt_',
    101: 'set_articlestats_dbt_',
    102: 'bigview_table_week_dbt_',
    103: 'bigview_table_yesterday_dbt_',
    104: 'bigview_table_month_dbt_',
    105: 'bigview_header_yesterday_dbt_',
    106: 'bigview_header_week_dbt_',
    107: 'bigview_traffic_week_dbt_',
    108: 'bigview_traffic_yesterday_dbt_',
}



traffic_channel_procedure_name = [
'Traffic_dynamic_linequery_month_dbt_',
'Traffic_dynamic_linequery_week_dbt_',
'Traffic_dynamic_linequery_day_dbt_',
'Traffic_pageview_week_dbt_',
'Traffic_pageview_month_dbt_',
'Traffic_pageview_ytd_dbt_',

'Traffic_channel_ytd_dbt_',
'Traffic_channel_month_dbt_',
'Traffic_channel_week_dbt_',

'Traffic_channel_forside_week_dbt_',
'Traffic_channel_forside_month_dbt_',
'Traffic_channel_forside_ytd_dbt_',

'Traffic_channel_week_newsletter_dbt_',
'Traffic_channel_month_newsletter_dbt_',
'Traffic_channel_ytd_newsletter_dbt_'
]

# Get database connection
def update_proc_name(site):
    cursor, connection = get_database_connection()
    if cursor and connection:
        try:
            # Loop through the dictionary and update the names
            for key, value in data_dict.items():
                if value is not None:
                    updated_value = f"{value}{site}"
                    update_query = f"UPDATE pre_stage.temp_mapping SET `query_name` = '{updated_value}' WHERE `query_id` = {key} AND siteid = {site};"
                    print(update_query)  # Print the query for debugging purposes
                    cursor.execute(update_query)  # Execute the update query
                else:
                    update_query = f"UPDATE pre_stage.temp_mapping SET `query_name` =  NULL WHERE `query_id` = {key} AND siteid = {site};"
                    print(update_query)  # Print the query for debugging purposes
                    cursor.execute(update_query)  # Execute the update query

            # Commit the changes
            connection.commit()
            print("All records updated successfully.")
        except mysql.connector.Error as error:
            print("Error executing the update queries:", error)
        finally:
            # Close the database connection
            cursor.close()
            connection.close()
    else:
        print("Failed to connect to the database.")


def drop_procedure(site):
    cursor, connection = get_database_connection()
    if cursor and connection:
        try:
            # Loop through the dictionary and update the names
            for key,value in data_dict.items():
                updated_value = f"{value}{site}"
                if value is not None:
                    drop_query = f"DROP PROCEDURE IF EXISTS {updated_value};"
                    print(key,": ",drop_query)  # Print the query for debugging purposes
                    cursor.execute(drop_query) 
                    connection.commit()
            # # Commit the changes
            print("All records deleted  successfully.")

            for value in traffic_channel_procedure_name:
                updated_value = f"{value}{site}"
                if value is not None:
                    drop_query = f"DROP PROCEDURE IF EXISTS {updated_value};"
                    print(value,": ",drop_query)  # Print the query for debugging purposes
                    cursor.execute(drop_query) 
                    connection.commit()



        except mysql.connector.Error as error:
            print("Error executing the update queries:", error)
        finally:
            # Close the database connection
            cursor.close()
            connection.close()
    else:
        print("Failed to connect to the database.")

  






if __name__ == "__main__":
    update_proc_name(20)
    # drop_procedure(17)

