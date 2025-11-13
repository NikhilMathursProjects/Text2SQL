# #--------------DB TABLE CHECKER----------------
# #checks what tables are in the db
# from db_setup import DatabaseSetup
# setup=DatabaseSetup({})
# setup.create_connection()
# setup.verify_database()

from dotenv import load_dotenv
import json
import os
import google.generativeai as genai

from llm_profiling import LLMProfilingSummarizer,EnhancedDatabaseProfiler

SAVE_FILE='llm_summary.json'
PROFILE_FILE= 'all_profiles101.json'

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
# model = genai.GenerativeModel("gemini-2.0-flash-lite")
# model = genai.GenerativeModel("gemini-2.0-flash-lite")
model = genai.GenerativeModel("gemini-2.5-flash")

llm_profiler=LLMProfilingSummarizer(llm_client=model)
# enhanced_llm_profiler=EnhancedDatabaseProfiler(llm_client=model)


with open(PROFILE_FILE,'r') as f:
    all_profiles=json.load(f)

table_names=list(all_profiles.keys())
print(all_profiles[table_names[0]].keys())
# llm_profiler.summarize_table_profile(table_name=table_names[0],profile_data=all_profiles[table_names[0]])

# prompt=llm_profiler.create_profile_prompt(table_name=table_names[0],profile_data=all_profiles[table_names[0]])

# print("=="*50)
# print(prompt)

for table_name in table_names:
    #for each table
    prompt=llm_profiler.create_profile_prompt(table_name=table_name,profile_data=all_profiles[table_name])
    #now that we have a prompt for that table
    #pass to llm
    print(prompt)
    response = model.generate_content(prompt)
    result_text = response.text
    print(result_text)
    #direct json saving
    save_file=str(table_name)+'.json'


    with open(save_file,'w') as f:
        f.write(result_text)


for table_name in table_names:
    file_name=str(table_name)
    with open(file_name,'r') as f:
        json_data=f.read()
    
    json_data=json_data.strip().replace('```json','').replace('```', '').strip()
    parsed_json = json.loads(json_data)
    
    with open(file_name + '.json', 'w') as f:
        json.dump(parsed_json, f, indent=2)  # Use indent for pretty formatting