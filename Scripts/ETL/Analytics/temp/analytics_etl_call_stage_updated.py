from stage_transformations import insert_newsletter_data_into_stage_11, insert_newsletter_data_into_stage_13, insert_newsletter_data_into_stage_14, insert_newsletter_data_into_stage_15, insert_newsletter_data_into_stage_16, insert_newsletter_data_into_stage_17


def insert_into_stage(cursor, connection, site_id, domain):
    # Insert data based on site id



    if id == 11:
        insert_newsletter_data_into_stage_11(cursor, connection, site_id, domain)
    elif id == 13:
        insert_newsletter_data_into_stage_13(cursor, connection, site_id, domain)
    elif id == 14:
        insert_newsletter_data_into_stage_14(cursor, connection, site_id, domain)
    elif id == 15:
        insert_newsletter_data_into_stage_15(cursor, connection, site_id, domain)
    elif id == 16:
        insert_newsletter_data_into_stage_16(cursor, connection, site_id, domain)
    elif id == 17:
        insert_newsletter_data_into_stage_17(cursor, connection, site_id, domain)
    elif id == 14:
        insert_newsletter_data_into_stage_14(cursor, connection, site_id, domain)