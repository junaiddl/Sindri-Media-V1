import pandas as pd
import mysql.connector
import json
import requests
from decimal import Decimal
from pympler import asizeof
import yaml
import os
from datetime import datetime, timedelta


def replace_double_quotes_with_single(json_string):
    """
    Replace double quotes with single quotes in a JSON string.

    Args:
        json_string (str): The JSON string with double quotes.

    Returns:
        str: The JSON string with double quotes replaced by single quotes.
    """
    # Parse the JSON string to ensure it's valid
    try:
        parsed_json = json.loads(json_string)
    except json.JSONDecodeError:
        raise ValueError("The provided string is not a valid JSON.")

    # Convert parsed JSON back to string with single quotes
    json_str_with_single_quotes = json.dumps(parsed_json, indent=1, ensure_ascii=False)

    # Replace double quotes with single quotes
    json_str_with_single_quotes = json_str_with_single_quotes.replace('"', "'")

    return json_str_with_single_quotes



def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host = '51.158.56.32',
            port = 1564,
            user = 'Site',
            password = '515B]_nP0;<|=pJOh35I'
        )
        cursor = connection.cursor()
        return cursor, connection
        


    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)



def barchart_query(procedure_name, currentSite, label, hint, realreferrer, tags_1,tags_2):
    domain = get_domain(currentSite)
    print(procedure_name, currentSite, label, hint, realreferrer, tags_1,tags_2)
    query = f"call prod.{procedure_name}({currentSite}, '''{label}''', '''{hint}''', '{realreferrer}', '''{domain}''');"
    
    print(query)

    cursor,connection = get_database_connection()
    cursor.execute(query)
    response = cursor.fetchall()

    return response



def get_domain(currentSite):
    query = f"select domain from prod.domain_siteid where siteid = {currentSite}"
    cursor, connection = get_database_connection()
    cursor.execute(query)
    dom = cursor.fetchone()
    return dom[0] if dom else None
    
    return dom



def channel_query(procedure_name,
                  currentSite,
                  realreferrer,
                  card_label,
                  card_hint,
                  chartbar_label,
                  chartbar_name,
                  chartbar_hint,
                  chartline_label,
                  chartline_hint):

    query = (
        f"call prod.{procedure_name}({currentSite}, {realreferrer}, "
        f"'''{card_label}''', '''{card_hint}''', '''{chartbar_label}''', '''{chartbar_name}''','''{chartbar_hint}''', "
        f"'''{chartline_label}''', '''{chartline_hint}''');"
    )
    print(query)


    cursor,connection = get_database_connection()
    cursor.execute(query)
    result = cursor.fetchall()
    
    if result:
        json_result = json.loads(result[0][0]) 
        return json_result

    return None



def fetch_inputs(currentSite, query_type):
    query = f"select params from pre_stage.input_params where siteid = {currentSite} and QUERY_TYPE = '{query_type}'"
    cursor, connection = get_database_connection()
    cursor.execute(query)
    cred = cursor.fetchall()

    if cred:
        json_string = cred[0][0]
        return json.loads(json_string)
    else:
        return None
    


def get_forside_channel(procedure_forside,
                        currentSite,
                        card_forside_label,
                        card_forside_hint,
                        chartbar_label_forside,
                        chartbar_hint_forside,
                        chartbar_name_forside,
                        chartline_forside_label,
                        chartline_forside_hint):
    

    query = (
        f"call prod.{procedure_forside}({currentSite}, '''Direct''' , "
        f"'''{card_forside_label}''', '''{card_forside_hint}''', '''{chartbar_label_forside }''', '''{chartbar_hint_forside}''', '''{chartbar_name_forside}''',"
        f"'''{chartline_forside_label}''', '''{chartline_forside_hint}''');"
    )


    print(query)
    cursor,connection = get_database_connection()
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        json_result = json.loads(result[0])  # Assuming the JSON is in the first column
        return json_result
    return None



def float_precision(obj):
    if isinstance(obj, float):
        return format(obj, '.15g')  # Adjust the precision as needed
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError



def defaulttitle_json(label, hint, categories, series, titles):
    # Start with the basic structure
    default = {
        "defaultTitle": titles[0],
        "label": label,
        "hint": hint,
        "categories": categories[0],
        "series": series[0],
        "additional": []
    }

    # Loop through the remaining titles, categories, and series
    for i in range(1, len(titles)):
        additional_entry = {
            "title": titles[i],
            "data": {
                "label": label,
                "categories": categories[i],
                "series": series[i]
            }
        }
        default["additional"].append(additional_entry)

    return default

def additional_json(label, hint, categories, series,title):
    add = {
         "title":title,
        "data": {
             "label": label,
             "hint": hint,
             "categories": categories, 
             "series": series,
                
                 }}
    return add
  


siteid = [13]

linechart_label = 'Besøg'
linechart_hint = 'Besøg opdelt på trafikkanaler'


card_label = "Besøg"
card_hint = "Antal besøg fra valgte trafikkanal samt udvikling ift. forrige sammenlignelige periode"
chartbar_label = "Ugerytme"
chartbar_hint = "Fordeling af besøg på ugedage for den valgte trafikkanal"
chartbar_name = "andel besøg"
chartline_label = "Døgnrytme"
chartline_hint = "Fordeling af besøg på timer i døgnet for hhv. hverdage og weekend for den valgte trafikkanal"
card_forside_label = "Kliks på forsiden"
card_forside_hint = "Antal kliks på forsiden i perioden samt udvikling ift. forrige sammenlignelige periode"
chartline_forside_label = "Døgnrytme"
chartline_forside_hint = "Fordeling af kliks på forsiden på timer i døgnet for hhv. hverdage og weekend."
chartbar_name_forside = "andel besøg"
chartbar_label_forside = "Ugerytme"
chartbar_hint_forside = "Fordeling af kliks på forsiden på ugedage"




def  get_output(inputs, barchart_name, channel_procedure, forside_channel_procedure, currentSite):
    label =  '''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'''
    hint = '''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'''
    title_second= "Brugerbehov"
    def_title = "Sektion"
    title = "Section"
    pageviews_def = ""
    
    main_json = {
         "site": currentSite, 
             "data": {
                 "pageviews": pageviews_def,
                 "channels":  []
             
             }
         }
    if len(inputs['pageview_input']) >= 1:
        for key, value in inputs['pageview_input'].items():
            if key == "default_d":
                r_referrer ="''Forside'',''Search'', ''Facebook'',''X'',''Newsletter''"
                r_tags_one = "''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig''"
                r_tags_second = "''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage''"
               
                
                categories_json = []
                series_data_json = []
                titles_json = []

                response = barchart_query( barchart_name ,currentSite, label, hint, r_referrer, r_tags_one,r_tags_second) 
                data = json.loads(response[0][0])
                print(data)

                default_title = data.get('defaultTitle')
                hint = data.get('hint')
                label = data.get('label')
                series = data.get('series')
                categories = data.get('categories')

                categories_json.append(categories)
                series_data_json.append(series)
                titles_json.append(default_title)

                # Print the extracted values
                print("defaultTitle:", default_title)
                print("hint:", hint)
                print("label:", label)
                print("series:", series)
                print("categories:", categories)

                for item in data['additional']:
                    title = item['title']
                    categories = item['data']['categories']
                    series = item['data']['series']
                    
                    print("Title:", title)
                    print("Categories:", categories)
                    print("Series:", series)
                    print()  # Print a blank line between each item


                for item in data['additional']:
                    title = item['title']
                    cate = item['data']['categories']
                    ser = item['data']['series']      
                    categories_json.append(cate)
                    series_data_json.append(ser)
                    titles_json.append(title)
 
                pageviews_def = defaulttitle_json(label, hint, categories_json, series_data_json, titles_json) # 
                # result = replace_double_quotes_with_single(response[0][0])
                # print(result)
                print(pageviews_def)
                main_json['data']['pageviews'] = pageviews_def
                
                
            else:
                r_referrer ="''Direct'',''Search'', ''Facebook'',''X'',''Newsletter''"
                r_tags_one = "''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig''"
                r_tags_second = "''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage''"
                response = barchart_query( barchart_name ,currentSite, label, hint, r_referrer, r_tags_one,r_tags_second) 
                add = additional_json(lab, hint, cat, series,title)
                main_json['data']['pageviews']['additional'].append(add)
                
    
    forside = get_forside_channel(forside_channel_procedure,
                                  currentSite,
                                  card_forside_label,
                                  card_forside_hint,
                                  chartbar_label_forside,
                                  chartbar_hint_forside,
                                  chartbar_name_forside,
                                  chartline_forside_label,
                                  chartline_hint)
    main_json['data']['channels'].append(forside)

    yaml_path = os.path.join(os.path.dirname(__file__),'shawarma.yml')

    # Read the YAML file
    with open(yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    # Extract the value for key 17
    vals_str = config['vals'][currentSite]

    # Convert the string to a list (since the value is a string)
    vals_list = [val.strip() for val in vals_str.split(',')]


    # for val in inputs['channels']['realreferrer']:
    for val in vals_list:
        print(val)
        channels = channel_query(channel_procedure,
                                 currentSite,
                                 val,
                                 card_label,
                                 card_hint,
                                 chartbar_label,
                                 chartbar_name,
                                 chartbar_hint,
                                 chartline_label,
                                 chartline_forside_hint)
        if channels:
            main_json['data']['channels'].append(channels)
    
    main_json_str = json.dumps(main_json, ensure_ascii=False, default=float_precision)
    return main_json_str




def line_charts(procedure_name, currentSite,label,hint):
    referrers = "''Direct'',''Search'',''Facebook'', ''Newsletter'', ''NL''"
    output_ref =  "''Direct'',''Search'',''Facebook'',''Newsletter''"

    query = f"call prod.{procedure_name}({currentSite}, '''{label}''', '''{hint}''', '{referrers}','{output_ref}');"
    print(query)
    
    cursor,connection = get_database_connection()
    cursor.execute(query)
    result = cursor.fetchall()
    
    if result:
        json_result = json.loads(result[0][0])  
        return json_result

    return None


def working(barchart_procedure, channel_procedure, forside_channel_procedure, currentSite):
   
    print('start')
    input_values = fetch_inputs(12,'month')
    print(input_values)
    result= get_output(input_values, barchart_procedure, channel_procedure, forside_channel_procedure, currentSite)
    
    final = json.loads(result)
    variable_size = ((asizeof.asizeof(final))/1024)/1024
    print(f"The size of the variable is: {variable_size} GB")
    return final


def getToken():
    url = 'https://sindri.media/strapi/api/auth/local'
    payload = {
        'identifier': 'import@dashboardapp.com',
        'password': 'JSyzkFtR9Cw5v5t'
    }

    auth_response = requests.post(url, data=payload)


    token = auth_response.json().get('jwt')
    return token


def post_data(day_chart,week_chart,month_chart,week_result,month_result,ytd_result):
    
    token = getToken()
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    day_url_chartline = 'https://sindri.media/api/v1/set-widget/trafikkanaler::visits::chart::day/'
    week_url_chartline = 'https://sindri.media/api/v1/set-widget/trafikkanaler::visits::chart::week/'
    month_url_chartline = 'https://sindri.media/api/v1/set-widget/trafikkanaler::visits::chart::month/'

    week_url = 'https://sindri.media/api/v1/set-widget/trafikkanaler::stats::object::day/'
    month_url = 'https://sindri.media/api/v1/set-widget/trafikkanaler::stats::object::month/'
    ytd_url = 'https://sindri.media/api/v1/set-widget/trafikkanaler::stats::object::ytd/'
    
    response = requests.post(day_url_chartline, headers=headers, json=day_chart)
    print("Response Status Code for Chartline Day:", response.status_code)
    print("Response Content:", response.text)
    
    response = requests.post(week_url_chartline, headers=headers, json=week_chart)
    print("Response Status Code for Chartline Week:", response.status_code)
    print("Response Content:", response.text)
    
    response = requests.post(month_url_chartline, headers=headers, json=month_chart)
    print("Response Status Code for Chartline Month:", response.status_code)
    print("Response Content:", response.text)
    
    response = requests.post(week_url, headers=headers, json=week_result)
    print("Response Status Code for Combined Week:", response.status_code)
    print("Response Content:", response.text)
    
    response = requests.post(month_url, headers=headers, json=month_result)
    print("Response Status Code for  Combined Month:", response.status_code)
    print("Response Content:", response.text)
    
    response = requests.post(ytd_url, headers=headers, json=ytd_result)
    print("Response Status Code for Combined YTD:", response.status_code)
    print("Response Content:", response.text)

def send_data_quality(id, status):

    # Get current date and time
    current_date = (datetime.now()).strftime('%Y-%m-%d')
    
    try:
        # Connect to the MySQL database
        cursor, connection = get_database_connection()

        # Insert error log into the data_quality_logs table
        insert_query = """
        INSERT INTO prod.posting_status (siteid, posting_date, traffic_status)
        VALUES (%s,%s,%s)
        """
        data = (id, current_date, status)  # False indicates an error occurred

        cursor.execute(insert_query, data)
        connection.commit()

        print(f"Inserting into DQ log for ID {id} at {current_date}")

    except mysql.connector.Error as db_err:
        print(f"Failed to insert into DQ log: {db_err}")

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def truncate_data_quality(site):
    cursor, connection = get_database_connection()

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    print(current_date)

    sql_query = f"""DELETE FROM prod.posting_status WHERE posting_date = '{current_date}' AND swagger_status IS NULL and siteid = {site}"""
    
    cursor.execute(sql_query)
    connection.commit()  # Commit the transaction to save the changes
    print(cursor.rowcount, "record(s) deleted.")

    cursor.close()
    connection.close()

def start_execution(id): 
    for sites in [id]:
        truncate_data_quality(sites)
        try:
            print(f"Started posting for Site ID: {sites}")

            day_chart = line_charts(f'Traffic_dynamic_linequery_day_dbt_{sites}',sites,linechart_label,linechart_hint)
            week_chart = line_charts(f'Traffic_dynamic_linequery_week_dbt_{sites}',sites,linechart_label,linechart_hint)
            month_chart = line_charts(f'Traffic_dynamic_linequery_month_dbt_{sites}',sites,linechart_label,linechart_hint)
                
            week_result = working(f'Traffic_pageview_week_dbt_{sites}', f'Traffic_channel_week_dbt_{sites}', f'Traffic_channel_forside_week_dbt_{sites}', sites)
            month_result = working(f'Traffic_pageview_month_dbt_{sites}',f'Traffic_channel_month_dbt_{sites}',f'Traffic_channel_forside_month_dbt_{sites}', sites)
            ytd_result = working(f'Traffic_pageview_ytd_dbt_{sites}', f'Traffic_channel_ytd_dbt_{sites}', f'Traffic_channel_forside_ytd_dbt_{sites}', sites)

            # day_chart['site'] = 2
            # week_chart['site'] = 2
            # month_chart['site'] = 2
                
            # week_result['site'] = 2
            # month_result['site'] = 2
            # ytd_result['site'] = 2

            post_data(day_chart,week_chart,month_chart,week_result,month_result,ytd_result)
            send_data_quality(sites, True)
        except Exception as e:
            # If any exception occurs, handle it here
            print(f"An error occurred while posting for Site ID: {sites}")
            print(e)
            send_data_quality(sites, False)