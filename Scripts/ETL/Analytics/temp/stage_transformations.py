import mysql.connector
import time

def insert_newsletter_data_into_stage_4(cursor, connection,siteid):
        print('Data insertion start into Stage')
        try:
            query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
                        SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
                        FROM pre_stage.newsletter  a
                        LEFT JOIN prod.site_archive_post b ON a.url = b.Link
                        WHERE  a.site_id = {siteid};
            """
            update_link_query14 =f"""
                UPDATE pre_stage.newsletter a
                        SET a.url = 
                            CASE
                                WHEN a.url REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.url, '?', 1)
                                ELSE a.url
                            END
                        WHERE a.site_id = {siteid}
            """
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
                cursor.execute(update_link_query14)
                cursor.execute(query)
                connection.commit()
                cursor.execute(query1)
                connection.commit()
                print("Insertion complete into Stage")
            except Exception as error:
                print(f"Error: {error}")
        except Exception as e:
            print("An error occurred:", str(e))


def insert_newsletter_data_into_stage_11(cursor, connection, siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
            SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
            FROM pre_stage.newsletter  a
            LEFT JOIN prod.site_archive_post b ON a.url = b.Link
            WHERE  a.site_id = {siteid};"""

        query1 = f"""
        UPDATE stage.newsletter
        SET referrer_name = 
            CASE

                -- Newsletter
                    WHEN referrer_type = 'mail-a'
                        THEN 'NL Red'
                    WHEN referrer_type = 'mail-b'
                        THEN 'NL Fag'
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




def insert_newsletter_data_into_stage_13(cursor, connection, siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
                    SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
                    FROM pre_stage.newsletter  a
                    LEFT JOIN prod.site_archive_post b ON a.url = b.Link
                    WHERE  a.site_id = {siteid};
        """
        update_link_query14 =f"""
            UPDATE pre_stage.newsletter a
                    SET a.url = 
                        CASE
                            WHEN a.url REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.url, '?', 1)
                            ELSE a.url
                        END
                    WHERE a.site_id = {siteid}
        """
        print('insert_data_into_stage for site 13')

        query1 = f"""
            UPDATE stage.newsletter
                SET referrer_name = 
                    CASE

                        -- Newsletter
                        WHEN referrer_type = 'mail-a'
                                THEN 'NL Medlem'
                        WHEN referrer_type = 'mail-b'
                                THEN 'NL fra NNF'
                        WHEN referrer_type like 'mail%'
                                THEN 'Newsletter'

                        -- Other
                        ELSE 'Other'
                    END
                where site_id = {siteid};
        """ 
        try:
            cursor.execute(update_link_query14)
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()
            print("Insertion complete into Stage")
        except Exception as error:
            print(f"Error: {error}")  
    except Exception as e:
        print("An error occurred:", str(e))





def insert_newsletter_data_into_stage_14(cursor, connection, siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
                SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
                FROM pre_stage.newsletter a
                LEFT JOIN prod.site_archive_post b ON a.url = concat(b.Link,'/')
                WHERE  a.site_id = {siteid};
        """

        remove_article_query14=f"""
            update pre_stage.newsletter SET url = REPLACE(url, '/artikel', '')
            WHERE url LIKE '%/artikel%' and site_id ={siteid};
        """
        
        update_link_query14 =f"""
            UPDATE pre_stage.newsletter a
                    SET a.url = 
                        CASE
                            WHEN a.url REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.url, '?', 1)
                            ELSE a.url
                        END
                    WHERE a.site_id = {siteid}
        """
        print('insert_data_into_stage for site 14')

        query1 = f"""
            UPDATE stage.newsletter
            SET referrer_name = 
                CASE

                    -- Newsletter
                    WHEN referrer_type = 'mail-a'
                        THEN 'NL <47'
                    WHEN referrer_type = 'mail-b'
                        THEN 'NL 48-60'
                    WHEN referrer_type = 'mail-c'
                        THEN 'NL >60'
                    WHEN referrer_type like 'mail%'
                        THEN 'Newsletter'

                    -- Other
                    ELSE 'Other'
                    END
                    where site_id = {siteid};
        """ 

        try:
            cursor.execute(update_link_query14)
            cursor.execute(remove_article_query14)
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()
            print("Insertion complete into Stage")
        except Exception as error:
            print(f"Error: {error}")  
    except Exception as e:
        print("An error occurred:", str(e))






def insert_newsletter_data_into_stage_15(cursor, connection ,siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""
            INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
            SELECT 
                a.referrer_type, 
                a.referrer_name, 
                a.site_id, 
                b.ID AS post_id, 
                a.url, 
                a.visits, 
                a.date
            FROM pre_stage.newsletter a
            LEFT JOIN prod.site_archive_post b 
                ON TRIM(TRAILING '/' FROM a.url) = TRIM(TRAILING '/' FROM b.Link)
            WHERE a.site_id = {siteid};
        """

        query1 = f"""
        UPDATE stage.newsletter
        SET referrer_name = 
            CASE

                -- Newsletter
                    
                    WHEN referrer_type ='mail-a'
                        THEN 'NL Hoved'
                    WHEN referrer_type = 'mail-b'
                        THEN 'NL Tillid'

                    WHEN referrer_type = 'mail-c'
                        THEN 'NL Leder'
                    WHEN referrer_type = 'mail-d'
                        THEN 'NL Pleje'
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
            print("Insertion complete into Stage")
        except mysql.connector.Error as error:
            print(f"Error: {error}")
            connection.close()
        print("Insertion complete into Stage")
    except Exception as e:
        print("An error occurred:", str(e))




def insert_newsletter_data_into_stage_16(cursor, connection ,siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
                    SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
                    FROM pre_stage.newsletter  a
                    LEFT JOIN prod.site_archive_post b ON a.url = b.Link
                    WHERE  a.site_id = {siteid};
        """
        update_link_query14 =f"""
            UPDATE pre_stage.newsletter a
                    SET a.url = 
                        CASE
                            WHEN a.url REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.url, '?', 1)
                            ELSE a.url
                        END
                    WHERE a.site_id = {siteid}
        """

        query1 = f"""
            UPDATE stage.newsletter
            SET referrer_name = 
                CASE
                    
                    -- Newsletter
                    WHEN referrer_type = 'mail-a'
                        THEN 'NL'
                    WHEN referrer_type like 'mail%'
                        THEN 'Newsletter'
                    -- Other
                    ELSE 'Other'
                    END
                    where site_id = {siteid};
        """ 
        try:
            cursor.execute(update_link_query14)
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()
            print("Insertion complete into Stage")
        except Exception as error:
            print(f"Error: {error}") 
    except Exception as e:
        print("An error occurred:", str(e))




def insert_newsletter_data_into_stage_17(cursor, connection,siteid):
    print('Data insertion start into Stage')
    try:
        query = f"""INSERT INTO stage.newsletter (referrer_type, referrer_name, site_id, post_id, url, visits, date)
                    SELECT a.referrer_type, a.referrer_name, a.site_id, b.ID AS post_id, a.url, a.visits, a.date
                    FROM pre_stage.newsletter  a
                    LEFT JOIN prod.site_archive_post b ON a.url = b.Link
                    WHERE  a.site_id = {siteid};
        """
        update_link_query14 =f"""
            UPDATE pre_stage.newsletter a
                    SET a.url = 
                        CASE
                            WHEN a.url REGEXP '\\\\?' THEN SUBSTRING_INDEX(a.url, '?', 1)
                            ELSE a.url
                        END
                    WHERE a.site_id = {siteid}
        """
        query1 = f"""
        UPDATE stage.newsletter
        SET referrer_name = 
            CASE

                -- NL H
                WHEN referrer_type = 'mail-a'
                    THEN 'NL H'

                -- NL S
                WHEN referrer_type = 'mail-b'
                    THEN 'NL S'

                -- NL F
                WHEN referrer_type = 'mail-c'
                    THEN 'NL F'

                -- NL B
                WHEN referrer_type = 'mail-d'
                    THEN 'NL B'

                -- NL C
                WHEN referrer_type = 'mail-e'
                    THEN 'NL C'

                -- Newsletter
                WHEN referrer_type like 'mail%'
                    THEN 'Newsletter'

                -- Other
                ELSE 'Other'
                END
                where site_id = {siteid};
        """ 

        try:
            cursor.execute(update_link_query14)
            cursor.execute(query)
            connection.commit()
            cursor.execute(query1)
            connection.commit()
            print("Insertion complete into Stage")
        except Exception as error:
            print(f"Error: {error}")
    except Exception as e:
        print("An error occurred:", str(e))