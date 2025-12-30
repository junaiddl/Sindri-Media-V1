
import os
import re
import mysql.connector
import logging
import json
from collections import defaultdict

def get_database_connection():
    return mysql.connector.connect(
        host = os.getenv("DB_HOST"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        port = int(os.getenv("DB_PORT")),
    )



# Setup logging to file with level DEBUG (you can change format as needed)
logging.basicConfig(
    filename='scenario_selection.log',
    filemode='w',  # overwrite each run
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# Define the metadata
# scenario_metadata = {
#     1: (1, 1), 2: (1, 3), 3: (1, 5), 4: (1, 2), 5: (1, 4), 6: (1, 6),
#     7: (2, 1), 8: (2, 2), 9: (3, 1), 10: (3, 2), 11: (2, 3),
#     12: (2, 4), 13: (3, 3), 14: (2, 5), 15: (3, 4), 16: (3, 5),
#     17: (3, 7), 18: (3, 6), 19: (3, 8), 20: (3, 9)
# }



# def run_sql_and_get_status(cursor, filepath):
#     with open(filepath, 'r') as file:
#         sql = file.read()
#     cursor.execute(sql)
#     result = cursor.fetchone()
#     return result[0] if result else False

# def select_scenarios(results):
#     logging.debug(f"Scenario results received: {results}")
#     grouped = defaultdict(list)
#     for scenario_id, status in results.items():
#         if status == 'True' or status is True:
#             group, rank = scenario_metadata[scenario_id]
#             grouped[group].append((rank, scenario_id))

#     logging.debug(f"Grouped scenarios with True status: {grouped}")

#     if not grouped:
#         logging.info("No scenarios are True. Returning empty list.")
#         return []

#     # Sort each group's scenarios by rank ascending (1 is highest priority)
#     for group in grouped:
#         grouped[group].sort()

#     # Sort groups by group number ascending (1 is highest priority)
#     sorted_groups = sorted(grouped.keys())
#     logging.debug(f"Sorted groups with True scenarios: {sorted_groups}")

#     total_true_scenarios = sum(len(s) for s in grouped.values())

#     # Rule 3: If only one scenario is True overall, return that one
#     if total_true_scenarios == 1:
#         chosen = [grouped[sorted_groups[0]][0][1]]
#         logging.info(f"Only one scenario True overall: scenario {chosen[0]}. Returning single scenario.")
#         return chosen

#     # Rule 2: If only one group has True scenarios, pick top 2 from that group
#     if len(sorted_groups) == 1:
#         top_two = [sid for _, sid in grouped[sorted_groups[0]][:2]]
#         logging.info(f"Only one group ({sorted_groups[0]}) has True scenarios. Returning top two: {top_two}")
#         return top_two

#     # Rule 1: If multiple groups have True scenarios:
#     # Pick highest-ranked from the two highest-priority non-empty groups
#     chosen = [grouped[sorted_groups[0]][0][1], grouped[sorted_groups[1]][0][1]]
#     logging.info(f"Multiple groups have True scenarios. Returning top scenarios from two highest-priority groups: {chosen}")
#     return chosen

# def main():
#     folder_path = r"C:\Users\Admin\Desktop\part2-3\scenario_queries"
#     results = {}

#     connection = get_database_connection()
#     cursor = connection.cursor()

#     for filename in os.listdir(folder_path):
#         if filename.startswith("scenario_") and filename.endswith(".sql"):
#             match = re.match(r"scenario_(\d+)\.sql", filename)
#             if match:
#                 scenario_id = int(match.group(1))
#                 path = os.path.join(folder_path, filename)
#                 try:
#                     status = run_sql_and_get_status(cursor, path)
#                     results[scenario_id] = status
#                     logging.debug(f"Scenario {scenario_id} returned status: {status}")
#                 except Exception as e:
#                     logging.error(f"Error with scenario {scenario_id}: {e}")

#     cursor.close()
#     connection.close()

#     selected = select_scenarios(results)
#     output = {"selected_scenarios": selected}
#     print(json.dumps(output))

# if __name__ == "__main__":
#     main()






# --- Setup logging ---
logging.basicConfig(
    filename='scenario_selection.log',
    filemode='w',  # overwrite each run
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# --- Metadata mapping (group, rank) ---
scenario_metadata = {
    1: (1, 1), 2: (1, 3), 3: (1, 5), 4: (1, 2), 5: (1, 4), 6: (1, 6),
    7: (2, 1), 8: (2, 2), 9: (3, 1), 10: (3, 2), 11: (2, 3),
    12: (2, 4), 13: (3, 3), 14: (2, 5), 15: (3, 4), 16: (3, 5),
    17: (3, 7), 18: (3, 6), 19: (3, 8), 20: (3, 9)
}


# --- Run SQL and get status for a single siteid ---
def run_sql_and_get_status(cursor, filepath, siteid):
    with open(filepath, 'r') as file:
        sql = file.read()
    cursor.execute(sql, {"siteid": siteid})   # ✅ single placeholder
    result = cursor.fetchone()
    return result[0] if result else False


# # --- Scenario selection logic ---
# def select_scenarios(results):
#     logging.debug(f"Scenario results received: {results}")
#     grouped = defaultdict(list)

#     for scenario_id, status in results.items():
#         if status == 'True' or status is True:
#             group, rank = scenario_metadata[scenario_id]
#             grouped[group].append((rank, scenario_id))

#     logging.debug(f"Grouped scenarios with True status: {grouped}")

#     if not grouped:
#         logging.info("No scenarios are True. Returning empty list.")
#         return []

#     # Sort each group's scenarios by rank
#     for group in grouped:
#         grouped[group].sort()

#     # Sort groups by priority
#     sorted_groups = sorted(grouped.keys())
#     logging.debug(f"Sorted groups with True scenarios: {sorted_groups}")

#     total_true_scenarios = sum(len(s) for s in grouped.values())

#     # Rule 3: Only one True overall
#     if total_true_scenarios == 1:
#         chosen = [grouped[sorted_groups[0]][0][1]]
#         logging.info(f"Only one scenario True overall: scenario {chosen[0]}")
#         return chosen

#     # Rule 2: Only one group has True
#     if len(sorted_groups) == 1:
#         top_two = [sid for _, sid in grouped[sorted_groups[0]][:2]]
#         logging.info(f"Only one group ({sorted_groups[0]}) has True. Returning top two: {top_two}")
#         return top_two

#     # Rule 1: Multiple groups
#     chosen = [grouped[sorted_groups[0]][0][1], grouped[sorted_groups[1]][0][1]]
#     logging.info(f"Multiple groups have True. Returning top two: {chosen}")
#     return chosen


# # --- Main driver ---
# def main():
#     folder_path = r"C:\Users\Admin\Desktop\part2-3\scenario_queries"
#     siteids = [11, 13, 14, 15, 16, 17, 18, 19, 20]

#     connection = get_database_connection()
#     cursor = connection.cursor()

#     all_outputs = {}

#     # ✅ Loop over siteids
#     for siteid in siteids:
#         results = {}
#         logging.info(f"=== Processing siteid {siteid} ===")

#         for filename in os.listdir(folder_path):
#             if filename.startswith("scenario_") and filename.endswith(".sql"):
#                 match = re.match(r"scenario_(\d+)\.sql", filename)
#                 if match:
#                     scenario_id = int(match.group(1))
#                     path = os.path.join(folder_path, filename)
#                     try:
#                         status = run_sql_and_get_status(cursor, path, siteid)
#                         results[scenario_id] = status
#                         logging.debug(f"Scenario {scenario_id} (siteid {siteid}) returned status: {status}")
#                     except Exception as e:
#                         logging.error(f"Error with scenario {scenario_id}, siteid {siteid}: {e}")

#         # select scenarios for this siteid
#         selected = select_scenarios(results)
#         all_outputs[siteid] = selected
#         print(f"Siteid {siteid} -> Selected scenarios: {selected}")

#     cursor.close()
#     connection.close()

#     # ✅ Final output JSON
#     print(json.dumps(all_outputs, indent=2))


# if __name__ == "__main__":
#     main()



# --- Scenario selection logic ---
def select_scenarios(results):
    grouped = defaultdict(list)

    for scenario_id, status in results.items():
        if status == 'True' or status is True:
            group, rank = scenario_metadata[scenario_id]
            grouped[group].append((rank, scenario_id))

    if not grouped:
        return []

    # Sort each group's scenarios by rank
    for group in grouped:
        grouped[group].sort()

    # Sort groups by priority
    sorted_groups = sorted(grouped.keys())

    total_true_scenarios = sum(len(s) for s in grouped.values())

    # Rule 3: Only one True overall
    if total_true_scenarios == 1:
        return [grouped[sorted_groups[0]][0][1]]

    # Rule 2: Only one group has True
    if len(sorted_groups) == 1:
        return [sid for _, sid in grouped[sorted_groups[0]][:2]]

    # Rule 1: Multiple groups
    return [grouped[sorted_groups[0]][0][1], grouped[sorted_groups[1]][0][1]]


# --- Main driver ---
def start_execution(siteid):
    folder_path = r"C:\Users\Admin\Desktop\part2-3\scenario_queries"

    connection = get_database_connection()
    cursor = connection.cursor()

    results = {}
    for filename in os.listdir(folder_path):
        if filename.startswith("scenario_") and filename.endswith(".sql"):
            match = re.match(r"scenario_(\d+)\.sql", filename)
            if match:
                scenario_id = int(match.group(1))
                path = os.path.join(folder_path, filename)
                try:
                    status = run_sql_and_get_status(cursor, path, siteid)
                    results[scenario_id] = status
                except Exception as e:
                    print(f"⚠️ Error in scenario {scenario_id}, siteid {siteid}: {e}")
                    continue

    selected = select_scenarios(results)

    cursor.close()
    connection.close()

    # ✅ Return only the selected list for this siteid
    print(f"Siteid {siteid} → {selected}")
    return selected


if __name__ == "__main__":
    start_execution()