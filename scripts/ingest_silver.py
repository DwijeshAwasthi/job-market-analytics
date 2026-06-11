import os
import sys
import warnings
import glob
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    spark = SparkSession.builder \
        .appName("JobMarketSilverCleaning") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print(" Spark Session active for Silver Layer.")

    json_files = glob.glob("data/bronze/job_postings/batch_*.json")
    
    if not json_files:
        print(" No bronze batch files found!")
        spark.stop()
        return

    print(f" Found {len(json_files)} batch files. Reading via Native Python...")

    raw_strings = []
    for file_path in json_files:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    raw_strings.append(line.strip())

    print(f" Loaded {len(raw_strings)} raw string entries into memory.")

    string_rdd = spark.sparkContext.parallelize(raw_strings)
    raw_df = spark.read.json(string_rdd)
    
    initial_count = raw_df.count()
    print(f" Initial row count before cleaning: {initial_count}")


    print("Deduplicating and normalizing data fields...")

    deduped_df = raw_df.dropDuplicates(["slug"])


    cleaned_df = deduped_df \
        .withColumn("title", trim(lower(col("title")))) \
        .withColumn("company_name", trim(lower(col("company_name")))) \
        .withColumn("location", trim(lower(col("location")))) \
        .withColumn("remote", when(col("remote")==True, "Remote").otherwise("On-Site"))

    
    cleaned_count = cleaned_df.count()
    print(f" Cleaned unique row count: {cleaned_count}")

    print("\nVerification Preview (Cleaned Schema & Sample Text):")
    cleaned_df.select("company_name", "title", "location", "remote").show(5, truncate=False)

    print(" Stopping Spark session...")
    spark.stop()

if __name__ == "__main__":
    main()