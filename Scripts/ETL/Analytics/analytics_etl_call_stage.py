from analytics_etl_4_3_10_13_stage import insert_data_into_stage_4_3_10_13
from analytics_etl_11_stage import insert_data_into_stage_11
from analytics_etl_14_stage import insert_data_into_stage_14
from analytics_etl_15_16_17_stage import insert_data_into_stage_15_16_17
from analytics_etl_18_19_stage import insert_data_into_stage_18_19
from analytics_etls_string import insert_data_into_stage_string
from newsletter_stage_transformations import (insert_newsletter_data_into_stage_4, insert_newsletter_data_into_stage_11, 
                                              insert_newsletter_data_into_stage_13, insert_newsletter_data_into_stage_14, 
                                              insert_newsletter_data_into_stage_15, insert_newsletter_data_into_stage_16, 
                                              insert_newsletter_data_into_stage_17, insert_newsletter_data_into_stage_18, 
                                              insert_newsletter_data_into_stage_19,insert_newsletter_data_into_stage_20,
                                              insert_newsletter_data_into_stage_22,insert_newsletter_data_into_stage_21)
def insert_into_stage(cursor, connection, id):
    # Insert data based on site id
    if id in [4, 3, 10, 13]:
        insert_data_into_stage_4_3_10_13(cursor, connection, id)
    elif id in [15, 16, 17]:
        insert_data_into_stage_15_16_17(cursor, connection, id)
    elif id == 11:
        insert_data_into_stage_11(cursor, connection, id)
    elif id == 14:
        insert_data_into_stage_14(cursor, connection, id)
    elif id in [18, 19, 20, 21]:
        insert_data_into_stage_18_19(cursor, connection, id)
    elif id ==22:
        insert_data_into_stage_string(cursor, connection, id)

    if id == 4:
        insert_newsletter_data_into_stage_4(cursor, connection, id)
    elif id == 13:
        insert_newsletter_data_into_stage_13(cursor, connection, id)
    elif id == 15:
        insert_newsletter_data_into_stage_15(cursor, connection, id)
    elif id ==16:
        insert_newsletter_data_into_stage_16(cursor, connection, id)
    elif id == 17:
        insert_newsletter_data_into_stage_17(cursor, connection, id)
    elif id == 14:
        insert_newsletter_data_into_stage_14(cursor, connection, id)
    elif id == 11:
        insert_newsletter_data_into_stage_11(cursor, connection, id)
    elif id == 18:
        insert_newsletter_data_into_stage_18(cursor, connection, id)
    elif id == 19:
        insert_newsletter_data_into_stage_19(cursor, connection, id)
    elif id == 20:
        insert_newsletter_data_into_stage_20(cursor, connection, id)
    elif id == 22:
        insert_newsletter_data_into_stage_22(cursor, connection, id)
    elif id == 21:
        insert_newsletter_data_into_stage_21(cursor, connection, id)