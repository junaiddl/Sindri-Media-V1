from analytics_etl_4_3_10_13_stage import insert_data_into_stage_4_3_10_13
from analytics_etl_11_stage import insert_data_into_stage_11
from analytics_etl_14_stage import insert_data_into_stage_14
from analytics_etl_15_16_17_stage import insert_data_into_stage_15_16_17
from analytics_etl_18_19_stage import insert_data_into_stage_18_19

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
    elif id in [18, 19]:
        insert_data_into_stage_18_19(cursor, connection, id)
