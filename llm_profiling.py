import json
import pandas as pd
import sqlite3
import google.generativeai as genai
import json
import os
import re
from dotenv import load_dotenv
from db_profiling import DatabaseProfiler
import json
class LLMProfilingSummarizer:
    def __init__(self, llm_client):
        """
        Just inititalizes the llm client (openai, gemini etc)
        """
        self.llm_client = llm_client


    #next func
    def create_profile_prompt(self, table_name, profile_data):
        """Create a comprehensive prompt for all columns in a table"""
        
        prompt = f"""You are a database expert analyzing the table '{table_name}' with {profile_data['row_count']} rows.

For each column below, analyze the profiling data and provide:
1. A SHORT description (1-2 sentences) explaining the column's meaning and purpose
2. A LONG description (more detailed) including format specifics, value patterns, and usage context

Format your response as a JSON object where keys are column names and values have "short_description" and "long_description".

Here is the profiling data:

"""
        
        for col_name, col_data in profile_data['columns'].items():
            prompt += f"\n--- Column: {col_name} ---\n"
            prompt += f"Data Type: {col_data['data_type']}\n"
            prompt += f"Null Values: {col_data['null_count']} out of {profile_data['row_count']}\n"
            prompt += f"Distinct Values: {col_data['distinct_count']}\n"
            
            if col_data['min_value'] is not None:
                prompt += f"Value Range: {col_data['min_value']} to {col_data['max_value']}\n"
            
            if 'min_length' in col_data and col_data['min_length'] is not None:
                prompt += f"Length Range: {col_data['min_length']} to {col_data['max_length']} characters\n"
            
            # if col_data['sample_values']:
            #     prompt += f"Sample Values: {col_data['sample_values'][:5]}\n"  # First 5 samples
            
            if 'common_patterns' in col_data and col_data['common_patterns']:
                prompt += f"Patterns: {col_data['common_patterns']}\n"
        
        prompt += """

IMPORTANT: Return ONLY valid JSON, no other text.DO NOT SAY ANY OTHER TEXT,ONLY VALID JSON, do NOT place these: '```' and do not write any other text.
example response:

{
  "column1": {
    "short_description": "Brief description",
    "long_description": "Detailed description with format specifics"
  },
  "column2": {
    "short_description": "Brief description", 
    "long_description": "Detailed description with format specifics"
  }
}

"""
        return prompt
    
    def summarize_table_profile(self, table_name, profile_data):
        """Get LLM summaries for all columns in a table in one call"""
        
        prompt = self.create_profile_prompt(table_name, profile_data)
        print("=="*50)
        print("PROMPT:",prompt)
        print("=="*50)
        try:
            # Adjust this based on your LLM client
            response = self.llm_client.generate_content(prompt)
            
            result_text = response.text
            
            # Parse JSON response
            print("parsing as json")
            summaries = json.loads(result_text)
            print("parsed as a json")
            # Validate structure
            for col_name in profile_data['columns'].keys():
                if col_name not in summaries:
                    raise KeyError(f"Missing column {col_name} in LLM response")
                if 'short_description' not in summaries[col_name]:
                    raise KeyError(f"Missing short_description for {col_name}")
                if 'long_description' not in summaries[col_name]:
                    raise KeyError(f"Missing long_description for {col_name}")
            
            return summaries
            
        except json.JSONDecodeError as e:
            print(f"Failed to parse LLM response as JSON: {e}")
            print(f"Raw response: {result_text}")
            # Fallback: try to extract JSON from response
            json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            raise
        except Exception as e:
            print(f"Error calling LLM: {e}")
            raise

# Enhanced DatabaseProfiler with LLM summarization
class EnhancedDatabaseProfiler(DatabaseProfiler):
    def __init__(self, db_path='cloud_costs.db', llm_client=None):
        # self.db_path = db_path
        # self.conn = sqlite3.connect(db_path)
        super().__init__(db_path)
        self.llm_summarizer = LLMProfilingSummarizer(llm_client) if llm_client else None
    
    def profile_table_with_llm(self, table_name):
        """Profile table and get LLM summaries for all columns"""
        # Get basic statistical profile
        profile = self.profile_table(table_name=table_name)
        print(profile)
        print("=="*50)

        # Get LLM summaries if available
        if self.llm_summarizer:
            try:
                print("summarizing")
                llm_summaries = self.llm_summarizer.summarize_table_profile(table_name, profile)
                print(llm_summaries)
                print("="*50)
                # Merge LLM summaries with profile data
                for col_name, col_data in profile['columns'].items():
                    if col_name in llm_summaries:
                        col_data.update(llm_summaries[col_name])
                
                profile['llm_summaries_generated'] = True
                print(profile)
                print("="*50)
            except Exception as e:
                print(f"Failed to get LLM summaries: {e}")
                profile['llm_summaries_generated'] = False
        else:
            profile['llm_summaries_generated'] = False
        
        return profile
    
# def main():
#     # Initialize LLM client (example with OpenAI)
#     llm_client = OpenAI(api_key="your-api-key")  # Replace with your actual client
    
#     # Create enhanced profiler
#     profiler = EnhancedDatabaseProfiler("your_database.db", llm_client)
    
#     # Profile a table with LLM summarization
#     table_profile = profiler.profile_table_with_llm("your_table_name")
    
#     # Save results
#     with open(f"{table_profile['table_name']}_profile.json", "w") as f:
#         json.dump(table_profile, f, indent=2, default=str)
    
#     # Print results
#     print(f"Profiled table: {table_profile['table_name']}")
#     for col_name, col_data in table_profile['columns'].items():
#         print(f"\n--- {col_name} ---")
#         if 'short_description' in col_data:
#             print(f"Short: {col_data['short_description']}")
#             print(f"Long: {col_data['long_description']}")
#         else:
#             print("No LLM summary available")
    
#     return table_profile

if __name__ == "__main__":
    load_dotenv()
    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    model = genai.GenerativeModel("gemini-2.0-flash-lite")
    enhancedprofiler=EnhancedDatabaseProfiler(llm_client=model)
    # table_name=['aws_cost_usage',]
    table_name=['azure_cost_usage','aws_cost_usage']
    all_profiles={}
    for table in table_name:
        all_profiles[table_name]=enhancedprofiler.profile_table_with_llm(table_name=table)
    print(all_profiles)
    with open('all_profiles.json','w') as f:
        json.dump(all_profiles,f,indent=4)
