import os
from dotenv import load_dotenv
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

load_dotenv()
print(os.getenv("URL"))

def fetch_data():
    url=os.getenv("URL")
    response=requests.get(url)
    if response.status_code==200:
        print("Data retrived")
        data_json=response.json()

        job_list= data_json.get('data', [])
        return job_list
    else:
        print(f"Error code{response.status_code}")
        return []

if __name__=="__main__":
    jobs=fetch_data()
    if jobs:
        print("\n\n Sample Job Structure:")
        print(jobs[0])