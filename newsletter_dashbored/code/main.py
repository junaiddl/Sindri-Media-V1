import shutil
import os
from unique_tokens import masterlist
from insertion import execution_clicks
from whoissub_insert import execution_whoissubscribed
from stats_insert import execution_stats
from newsletter_etl_v2 import execution

BASE_PATH = r"downloaded_files_v2" 
def cleanup():
    """Cleans up the downloaded files directory after processing."""
    if os.path.exists(BASE_PATH):
        shutil.rmtree(BASE_PATH)
        print(f"Cleaned up directory: {BASE_PATH}")
    else:
        print(f"Directory not found, nothing to clean: {BASE_PATH}")

def execute_all_functions():
    cleanup()
    execution(1)
    execution_clicks()
    execution_whoissubscribed()
    execution_stats()
    masterlist()
    cleanup()



if __name__ == "__main__":
    execute_all_functions()