import requests
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import os
Tk().withdraw()
filenames = askopenfilenames(filetypes=[("CSV files", "*.csv")])

files = []
for filename in filenames:
    # Use the basename for the upload but keep original for reference
    files.append(('files', (os.path.basename(filename), open(filename, 'rb'), 'text/csv')))

try:
    response = requests.post(
        "http://127.0.0.1:8000/upload_csv",
        files=files
    )
    
    print("Status Code:", response.status_code)
    if response.status_code == 200:
        data = response.json()
        print(data)
    else:
        print("Error:", response.text)
        
finally:
    for file_tuple in files:
        file_tuple[1][1].close()

queries=["Show me the total compute cost for Azure grouped by service.",
 "Which AWS region had the highest spend last month?",
 "What is the daily trend of S3 storage cost?",
 "What is EC2 usage by instance type?",
#  "Show cost by environment tag."
 ]

# for query in queries:
#     print("=="*50)
#     print("QUERY:",query)
#     response = requests.post(
#         "http://127.0.0.1:8000/text_to_sql",
#         json={"user_query": query}
# )
    # print("Status Code:", response.status_code)
    # print("Response JSON:", response.json())
