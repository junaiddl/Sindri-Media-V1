import mysql.connector
def insert_newsletter_data_into_stage_18(cursor, connection, siteid):
    remove_link_slash = f"""
            UPDATE pre_stage.newsletter
            SET url =  LEFT(url, LENGTH(url) - 1)
            WHERE url LIKE '%/' AND site_id ={siteid};
        """
    cursor.execute(remove_link_slash)
    connection.commit()


    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
            SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
            FROM pre_stage.newsletter a
            LEFT JOIN prod.site_archive_post b ON a.url = b.Link
            WHERE  a.site_id = {siteid};"""

        query1 = f"""
        UPDATE stage.newsletter
        SET referrer_name = 
            CASE

                -- Newsletter
                    WHEN referrer_type like 'mail%'
                        THEN 'Newsletter'

                -- Other
                ELSE 'Other'
                END
                where site_id = {siteid};
        """

        try:
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()

        except mysql.connector.Error as error:
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()
            print(f"Error: {error}")
        print("Insertion complete into Stage")
    except Exception as e:
        print("An error occurred:", str(e))