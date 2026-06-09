import os
import sys
from dotenv import load_dotenv
import warnings
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

load_dotenv()
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

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

def main():
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    raw_jobs=fetch_data()

    if not raw_jobs:
        print("No data to process. Existing")
        return
    spark=SparkSession.builder\
    .appName("JobMarketBronzeIngestion") \
    .getOrCreate()
    print("Spark session is active")
    spark.sparkContext.setLogLevel("ERROR")


    df=spark.createDataFrame(raw_jobs)
    bronze_df=df.withColumn("ingested_at", current_timestamp())\
    .withColumn("data_source", lit("arbeitnow_api"))

    print("Schema for bronze layer:")
    bronze_df.printSchema()

    print("\n Preview: ")
    local_rows = [row.asDict() for row in bronze_df.collect()]
    from datetime import datetime
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = f"data/bronze/job_postings/batch_{timestamp_str}.json"
    os.makedirs("data/bronze/job_postings", exist_ok=True)
    with open(output_file_path, "w", encoding="utf-8") as f:
        for row in local_rows:
            if 'ingested_at' in row and row['ingested_at'] is not None:
                row['ingested_at']= row['ingested_at'].isoformat()
            f.write(json.dumps(row)+ "\n")

    with open("data/bronze/job_postings/_SUCCESS", "w") as f:
        f.write("")

    
    print("Raw data landed in bronze layers successfullt")
    spark.stop()        

if __name__=="__main__":
    main()