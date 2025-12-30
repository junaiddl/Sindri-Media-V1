import mysql.connector
import json
import requests
from scenerio import scenerio_query
from executer import start_execution
from scenerios_7_20 import updated_queries
import os




def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
        )
        cursor = connection.cursor()
        return cursor, connection           
    except mysql.connector.Error as error:
        print("Error inserting data into MySQL database:", error)


def authenticate():
    try:
        url = 'https://sindri.media/strapi/api/auth/local'
        payload = {
            'identifier': 'import@dashboardapp.com',
            'password': 'JSyzkFtR9Cw5v5t'
        }
        auth_response = requests.post(url, data=payload)
        
        # Assuming the JWT token is returned in the 'token' field of the response JSON
        token = auth_response.json().get('jwt')
        return token
        
    except requests.exceptions.RequestException as req_err:
        print("Request error occurred:", req_err)
        return None
    except Exception as ex:
        print("An unexpected exception occurred:", ex)
        return None

def post(token,url,json_data_list):
    global status
    while token is None:  # Keep trying until a valid token is obtained
        print("Invalid or None token. Re-authenticating...")
        token = authenticate()
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        print('len: ',len(json_data_list))
        c = 0
        for json_data in json_data_list:
            response = requests.post(url, headers=headers, json=json_data)
            if response.status_code == 200:
                c=c+1
                print(response.text,c)
            elif response.status_code == 401:  # Unauthorized, possibly due to invalid token
                print("Unauthorized access. Refreshing token.")
                token = authenticate()
                while token is None:  # Keep trying until a valid token is obtained
                    print("Invalid or None token. Re-authenticating...")
                    token = authenticate()
                headers = {
                                'Authorization': f'Bearer {token}',
                                'Content-Type': 'application/json'}
                response = requests.post(url, headers=headers, json=json_data)
                if response.status_code == 200:
                    c = c + 1
                    print(response.text, c)
                else:
                    status = False
                    print(f'Error: {response.status_code} - {response.text}')
            else:
                print(f'Error: {response.status_code} - {response.text}')
    except Exception as e:
        print(e)
def scenerio_post(siteid, event, key, gns):
    # Get selected scenarios for this site (max 2)
    selected = start_execution(siteid)

    if not selected:
        print(f"No scenarios selected for siteid {siteid}")
        return None

    # --- Build query mapping ---
    sec_1,sec_2, sec_3 = scenerio_query(siteid, event, key, gns)
    scenario_7, scenario_8, scenario_9, scenario_10, scenario_11, scenario_12_20 = updated_queries(siteid, event)

    query_map = {}
    for sid in range(1, 7):
        query_map[sid] = sec_1
    query_map.update({
        7: scenario_7,
        8: scenario_8,
        9: scenario_9,
        10: scenario_10,
        11: scenario_11
    })
    for sid in range(12, 21):
        query_map[sid] = scenario_12_20

    # --- DB + auth ---
    cursor, connection = get_database_connection()
    token = authenticate()

    # Pick first and second scenario IDs
    scenario1_id = selected[0] if len(selected) > 0 else None
    scenario2_id = selected[1] if len(selected) > 1 else None
    print(f"Selected scenarios for siteid {siteid}: {scenario1_id}, {scenario2_id}")

    # --- Run first scenario ---
    result1 = None
    if scenario1_id:
        query1 = query_map.get(scenario1_id)
        print(f"➡️ Running Scenario {scenario1_id}")
        if query1:
            # print(query1)
            cursor.execute(query1)
            result1 = cursor.fetchall()
            result1 = json.loads(result1[0][0])
    print(result1)

    # --- Run second scenario ---
    result2 = None
    if scenario2_id:
        print(f"➡️ Running Scenario {scenario2_id}")
        query2 = query_map.get(scenario2_id)
        if query2:
            cursor.execute(query2)
            result2 = cursor.fetchall()
            result2 = json.loads(result2[0][0])

    # --- Final JSON structure ---
    token = authenticate()
    step = {
        "site": siteid,
        "uge": "uge39",
        "prev_uge": "uge38",
        "scenario1": str(scenario1_id) if scenario1_id else None,
        "scenario2": str(scenario2_id) if scenario2_id else None,
        "data1": result1,
        "data2": result2
    }
    json_data_list_1 = []
    json_data_list_1.append(step)
    url = f'https://sindri.media/api/v1/ugerapport/gen-recommend/' 
    # post(token,url,json_data_list_1)
    return step
def worker_function():
    ids = [11,13,14,15,16,17,18,19,20]
    results = []

    output_dir = "scenario_results"
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n📂 Output folder: {output_dir}")

    for siteid in ids:
        print(f"\n=== Processing siteid {siteid} ===")
        result = scenerio_post(siteid, 'Next Click', 'Next Click (%)', 'gns_next_click_%')

        if result:
            results.append(result)

            file_path = os.path.join(output_dir, f"site_{siteid}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)

            print(f"💾 Saved result for siteid {siteid} → {file_path}")

    print("\n🎉 All sites processed")
    return results


if __name__ == "__main__":
    worker_function()