from fastapi import FastAPI, HTTPException,UploadFile,Form,File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import google.generativeai as genai

import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
import csv
import codecs

import sqlite3
import pandas as pd
import json

from db_setup import DatabaseSetup
from db_profiling import DatabaseProfiler
from llm_profiling import LLMProfilingSummarizer

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
app = FastAPI()
class QueryRequest(BaseModel):
    user_query: str

# gemini-2.0-flash-lite #fast
# gemini-2.5-flash #slower
# model = genai.GenerativeModel("gemini-2.0-flash-lite")
model = genai.GenerativeModel("gemini-2.5-flash")

UPLOAD_DIR='uploads'
os.makedirs(UPLOAD_DIR,exist_ok=True)
SUMMARY_FILE = 'all_summaries.json'
PROFILE_FILE= 'all_profiles101.json'
LLM_PROFILE_DIR='separate_llm_profiles/' #contains all separate llm profile jsons
COMPLETE_SUMMARY_FILE = 'complete_profiles.json' #this one has the llm profiling with everything else as well


#------ for the main api--------
def get_profile_descriptions():
    """
    Gets complete profile descriptions.

    Args:
        - user_query

    Return:
        - profile map
    """
    #opens the file that has the profiles saved
    with open('complete_profiles.json','r') as f:
        all_profiles = json.load(f)
    print(all_profiles)
    
    #only uses the short descriptions for the data
    profile_map = {}
    for table in all_profiles.keys():
        profile_map[table] = {
            'row_count': all_profiles[table]['row_count'],
            'columns': {col: {
                'data_type': data['data_type'],
                'sample_values': data['sample_values'][:2],
                'short_description': data['short_description']
                # 'long_description':data['long_description'],
            } for col, data in all_profiles[table]['columns'].items()}
        }
    return profile_map

def execute_sql_query(query: str):
    """
    Connects to the database and performs the sql query, closes the db and returns the result in a list of rows.
    """
    print(f"EXECUTING SQL: {query}")
    #what i cant do is connect to the db for every execution of the sql query (im not5 sure if thats fine)
    conn = sqlite3.connect('cloud_costs.db')
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def generate_sql_from_natural_language(user_query: str):
    """
    Creates the sql query by querying the llm.

    Args:
        - user_query

    Returns:
        - sql_query
    """
    #gets profile dict
    profile_map = get_profile_descriptions()
    schema_context = ""
    for table_name, profile in profile_map.items():
        schema_context += f"\nTable: {table_name}\nRows: {profile['row_count']}\n"
        for col_name, col_data in profile['columns'].items():
            schema_context += f"  - {col_name}: {col_data['short_description']}\n"
    
    prompt = f"""Database Schema:
{schema_context}

Query: "{user_query}"

Return ONLY SQL:"""
    
    response = model.generate_content(prompt)
    sql_query = response.text.strip().replace('```sql', '').replace('```', '').strip()
    return sql_query

def generate_natural_language_answer(user_query: str, sql_results: list, sql_query: str):
    if not sql_results:
        return "No data found."
    
    prompt = f"""Query: {user_query}
SQL: {sql_query}
Results: {sql_results}

Answer:"""

    response = model.generate_content(prompt)
    return response.text.strip()

@app.post("/text_to_sql")
async def text_to_sql(request: QueryRequest):
    """
    This performs the actual text to sql answering.

    Args:
        - `user_query` is the user query bound by QueryRequest
    
    This function generates the sql query using the `user_query`, then executes it on the database
    Finally it generates the natural language answer from that data(Google Studio API).

    Returns:
        - Json Response :
            `user_query`
            `sql_query`
            `json_data`
            `answer`
            `records_returned`
    """
    sql_query = generate_sql_from_natural_language(request.user_query)
    sql_results = execute_sql_query(sql_query)
    natural_answer = generate_natural_language_answer(request.user_query, sql_results, sql_query)
    
    return JSONResponse(content={
        "user_query": request.user_query,
        "sql_query": sql_query,
        "json_data": sql_results,
        "answer": natural_answer,
        "records_returned": len(sql_results)
    })

#-------------main api complete-----------------------
def list_dirs(file_names:List[str]):
    """
    Returns the required map dict.
    """
    required_map={}
    for file in file_names:
        if file.endswith('.csv'):
            required_map[Path(file).stem]=Path(os.path.join(UPLOAD_DIR,file))
    return required_map
    
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


def load_existing_summaries(filepath):
    """Load existing all_summaries.json if present"""
    if os.path.exists(filepath):
        try:
            #tries to return tbe json (IF SUMMARY FILE NOT EMPTY)
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            print(f"Error(maybe file empty)")
            #NO EXISTING SUMMARY IN THE FILE CURRENTLY
            return {}
    return {}

def save_summaries(filepath, summaries):
    """Save JSON summaries with indentation"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(summaries, f, indent=4)
#all i need to do is add the data to the db, then perform the complete setup which makes a simple summary which i add to all_summaries.json
#then i need to perform the statistical basic profiling for that data and add it to all_profiles.json
#then i need to perform the llm profiling for the data and add it to ll_profiles.json or complete_profiles.json
#Then i can perform the normal text2 sql, i make 2 different apis:
    #1 which takes in the data and stores it with doing the db setup, profiling and llm profiling.
    #2 another which takes in data and performs all above and text2sql , so json data ={files:files, user_query:query}
#later i need to change the way i do the text2sql by using the sql probes

def update_db(filenames:list[str]):
    """
    Inserts the csv files as tables into the db:`cloud_costs.db`.
    Then it saves the simple summary of each table into the all_summaries.json file

    """
    #so im updating the db with the new files/even old files, so i need to check if this filename already exists then dont change unless the data is bigger
    #get the required map:
    required_map=list_dirs(filenames)
    print('Detected CSV:',required_map)
    setup=DatabaseSetup(required_map)
    setup.setup_complete()
    summary=setup.get_database_summary()
    all_profiles = load_existing_summaries(SUMMARY_FILE)
    print("Existing tables:", list(all_profiles.keys()) if all_profiles else "None")
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
    #till here it saves everything to all_summaries.json
    #now i have to perform the basic profiling on these specific tables
    #so since filenames is the complete one with '.csv' , ill have to pass just the table names
    table_names=list(required_map.keys())
    print("Detected Table names:",table_names)
    profiler=DatabaseProfiler(table_map=table_names)
    basic_profile=profiler.profile_all_tables() #another thing just like summary
    all_basic_profiles=load_existing_summaries(PROFILE_FILE)

    updated=0
    added=0
    skipped=0
    # for table_name,table_info in basic_profile.keys()
    for table_name, table_info in basic_profile.items():
        new_rows = table_info.get('row_count', 0)

        if table_name not in all_basic_profiles:
            all_basic_profiles[table_name] = table_info
            added += 1
            print(f"Added new table: {table_name} ({new_rows} rows)")
        else:
            old_rows = all_basic_profiles[table_name].get('row_count', 0)
            if new_rows > old_rows:
                all_basic_profiles[table_name] = table_info
                updated += 1
                print(f"Updated table: {table_name} ({old_rows} → {new_rows} rows)")
            else:
                basic_profile[table_name]=all_basic_profiles[table_name] #updating basic_profile dict
                skipped += 1
                print(f"Skipped {table_name} (old rows: {old_rows}, new rows: {new_rows})")
    save_summaries(PROFILE_FILE, all_basic_profiles)
    #this now saves the basic profiling summary to the PROFILE FILE
    #now i have to make the llm profiles using the llm_profiling.py class LLMProfilingSummarizer
    #i can also continue to use the `basic_profile` dict since i keep it updated
    llm_profiler=LLMProfilingSummarizer(llm_client=model)
    for table_name in table_names:
        prompt=llm_profiler.create_profile_prompt(table_name=table_name,profile_data=basic_profile[table_name])
        response = model.generate_content(prompt)
        result_text = response.text
        #path for the file to place the llm profile data into
        llm_profile_save_file=LLM_PROFILE_DIR+table_name+'.json'
        json_data=result_text.strip().replace('```json','').replace('```', '').strip()
        parsed_json = json.loads(json_data)

        with open(llm_profile_save_file, 'w') as f:
            json.dump(parsed_json, f, indent=2)
    #so now this saves the llm summaries to specific table name.json files in separate_llm_profiles
    #now i gotta compile it all together in complete_profiles.json
    #so i have the basic_profile dict
    #and the multiple files with table names llm _decriptions dicts when loaded
    # complete_summary_file=COMPLETE_SUMMARY_FILE #complete_profiles.json
    #just open each llm profile file and add it into the basic profile dict and at the end just write to the complete dict
    complete_summary=basic_profile.copy()
    for table_name in table_names:
        llm_profile_save_file=LLM_PROFILE_DIR+table_name+'.json'
        llm_profile=load_existing_summaries(llm_profile_save_file)
        # llm_profile[table_name]={short,long}
        for col_name,col_data in complete_summary[table_name]['columns'].items():
            # basic_profile[table_name]['columns'][col_name]=col_data
            col_data.update(llm_profile[col_name])
    save_summaries(COMPLETE_SUMMARY_FILE,complete_summary)


@app.post("/upload_csv")
async def uploads_data_to_db(files: list[UploadFile] = File(...)):
    """
    Saves the users csv file to the `upload` directory, then saves the data to the cloud_costs.db.
    """
    saved_files = {}
    filenames=[]
    for file in files:
        if not file.filename.lower().endswith('.csv'):
            continue
        
        # Get just the filename (not full path)
        filename = os.path.basename(file.filename)
        upload_location = os.path.join(UPLOAD_DIR, filename)
        
        # Read file content and save it
        content = await file.read()
        with open(upload_location, 'wb') as f:
            f.write(content)
        filenames.append(file.filename)

    update_db(filenames)
    return {"message": "Files uploaded successfully"}

    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)