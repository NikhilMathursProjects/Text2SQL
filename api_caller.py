import requests
queries=["Show me the total compute cost for Azure grouped by service.",
 "Which AWS region had the highest spend last month?",
 "What is the daily trend of S3 storage cost?",
 "What is EC2 usage by instance type?",
 "Show cost by environment tag."]

for query in queries:
    print("=="*50)
    print("QUERY:",query)
    response = requests.post(
        "http://127.0.0.1:8000/text_to_sql",
        json={"user_query": query}
)
    print("Status Code:", response.status_code)
    print("Response JSON:", response.json())