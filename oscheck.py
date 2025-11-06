import os
from pathlib import Path
from db_setup import DatabaseSetup
import json

UPLOAD_DIR = 'uploads'
SUMMARY_FILE = 'all_summaries.json'

def list_upload_dir():
    """Get list of all CSVs in upload_dir as {table_name: Path(...)}"""
    return {
        Path(file).stem: Path(os.path.join(UPLOAD_DIR, file))
        for file in os.listdir(UPLOAD_DIR)
        if file.endswith(".csv")
    }

def load_existing_summaries(summary_file):
    """Load existing all_summaries.json if present"""
    if os.path.exists(summary_file):
        try:
            #tries to return tbe json (IF SUMMARY FILE NOT EMPTY)
            with open(summary_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            print(f"Error(maybe file empty)")
            #NO EXISTING SUMMARY IN THE FILE CURRENTLY
            return {}
    return {}

def save_summaries(summary_file, summaries):
    """Save JSON summaries with indentation"""
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summaries, f, indent=4)

#Gets the required map(all data in the upload dir)
required_map = list_upload_dir()
print("Detected CSVs:", required_map)

#Database setup
setup = DatabaseSetup(required_map)
setup.setup_complete()
summary = setup.get_database_summary()  #gets simple summary

#loads existing summary from the summary file
all_profiles = load_existing_summaries(SUMMARY_FILE)
print("Existing tables:", list(all_profiles.keys()) if all_profiles else "None")

#merges summaries(no update issue)
updated = 0
added = 0
skipped = 0

for table_name, table_info in summary['tables'].items():
    new_rows = table_info.get('rows', 0)

    if table_name not in all_profiles:
        all_profiles[table_name] = table_info
        added += 1
        print(f"Added new table: {table_name} ({new_rows} rows)")
    else:
        old_rows = all_profiles[table_name].get('rows', 0)
        if new_rows > old_rows:
            all_profiles[table_name] = table_info
            updated += 1
            print(f"Updated table: {table_name} ({old_rows} → {new_rows} rows)")
        else:
            skipped += 1
            print(f"Skipped {table_name} (old rows: {old_rows}, new rows: {new_rows})")


save_summaries(SUMMARY_FILE, all_profiles)
