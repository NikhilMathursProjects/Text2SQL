from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import json
import google.generativeai as genai
import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

app = FastAPI()

class QueryRequest(BaseModel):
    user_query: str

model = genai.GenerativeModel("gemini-2.0-flash-lite")

def get_profile_descriptions():
    with open('all_profiles.json','r') as f:
        all_profiles = json.load(f)
    
    profile_map = {}
    for table in ['aws_cost_usage', 'azure_cost_usage']:
        profile_map[table] = {
            'row_count': all_profiles[table]['row_count'],
            'columns': {col: {
                'data_type': data['data_type'],
                'sample_values': data['sample_values'][:2],
                'short_description': data['short_description']
            } for col, data in all_profiles[table]['columns'].items()}
        }
    return profile_map

def execute_sql_query(query: str):
    print(f"EXECUTING SQL: {query}")
    conn = sqlite3.connect('cloud_costs.db')
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict('records')

def generate_sql_from_natural_language(user_query: str):
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
Results: {len(sql_results)} records

Answer:"""
    
    response = model.generate_content(prompt)
    return response.text.strip()

@app.post("/text_to_sql")
async def text_to_sql(request: QueryRequest):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)