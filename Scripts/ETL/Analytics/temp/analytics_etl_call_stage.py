from analytics_etl_4_10_13_stage import insert_data_into_stage_4_10_13
from analytics_etl_11_stage import insert_data_into_stage_11
from analytics_etl_14_stage import insert_data_into_stage_14
from analytics_etl_15_16_17_stage import insert_data_into_stage_15_16_17
from stage_transformations import insert_newsletter_data_into_stage_4, insert_newsletter_data_into_stage_11, insert_newsletter_data_into_stage_13, insert_newsletter_data_into_stage_14, insert_newsletter_data_into_stage_15, insert_newsletter_data_into_stage_16, insert_newsletter_data_into_stage_17
from newsletter_stage_transformations import insert_newsletter_data_into_stage_18
from analytics_etl_18_19_stage import insert_data_into_stage_18_19


def insert_into_stage(cursor, connection, site_id):
    print("Inserting Data Into Stage")
    # Insert data based on site id
    if site_id in [4, 10, 13]:
        insert_data_into_stage_4_10_13(cursor, connection, site_id)
    elif site_id in [15, 16, 17]:
        insert_data_into_stage_15_16_17(cursor, connection, site_id)
    elif site_id == 11:
        insert_data_into_stage_11(cursor, connection, site_id)
    elif site_id == 14:
        insert_data_into_stage_14(cursor, connection, site_id)
    elif site_id in [18, 19]:
        insert_data_into_stage_18_19(cursor, connection, site_id)


    if site_id == 4:
        insert_newsletter_data_into_stage_4(cursor, connection, site_id)
    elif site_id == 13:
        insert_newsletter_data_into_stage_13(cursor, connection, site_id)
    elif site_id == 15:
        insert_newsletter_data_into_stage_15(cursor, connection, site_id)
    elif site_id ==16:
        insert_newsletter_data_into_stage_16(cursor, connection, site_id)
    elif site_id == 17:
        insert_newsletter_data_into_stage_17(cursor, connection, site_id)
    elif site_id == 14:
        insert_newsletter_data_into_stage_14(cursor, connection, site_id)
    elif site_id == 11:
        insert_newsletter_data_into_stage_11(cursor, connection, site_id)
    elif site_id in (18, 19):
        insert_newsletter_data_into_stage_18(cursor, connection, site_id)





