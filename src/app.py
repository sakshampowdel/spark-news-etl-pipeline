from typing import List
import pathlib
import logging
import os
import json

from extraction.scraper import extract_to_bronze
from extraction.utils import load_bronze_records
from transformation.cleaner import process_bronze_to_silver
from transformation.analyzer import create_spark_session, generate_source_stats, generate_top_keywords

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("main")

def execute_bronze_pipeline(output_path: str) -> None:
  logger.info("Starting Bronze Layer...")

  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  with open(output_path, 'a', encoding='utf-8') as f:
    records_extracted: int = extract_to_bronze(f)

  if records_extracted == 0:
    logger.error("No records were extracted to Bronze!")
    raise RuntimeError("Bronze extraction failed: Zero records saved.")

  logger.info(f"Bronze layer complete. Total records persisted: {records_extracted}.")

def run_silver_layer(input_path: str, output_path: str) -> None:
  """
  Orchestrates the transformation of raw Bronze HTML records into structured Silver records.

  Args:
    input_path (str): The file path to the raw .jsonl bronze data.
    output_path (str): The file path where the cleaned .jsonl data will be saved.

  Raises:
    RuntimeError: If no records are found in the bronze source.
  """
  logger.info('Starting Silver Layer...')
  os.makedirs(os.path.dirname(output_path), exist_ok=True)
  bronze_records = load_bronze_records(input_path)

  with open(output_path, 'a', encoding='utf-8') as f:
    records_saved: int = process_bronze_to_silver(bronze_records, f)

  if records_saved == 0:
    logger.error("No records were saved to Silver!")
    raise RuntimeError("Error loading Silver records!")
  
  logger.info(f"Silver layer complete. Total records streamed: {records_saved}.")

def run_gold_layer(input_path: str, output_dir: str) -> None:
  """
  Orchestrates the aggregation of Silver records into Gold analytical tables using Spark.

  Args:
    input_path (str): The file path to the cleaned .jsonl silver data.
    output_dir (str): The directory where the Gold Parquet tables will be saved.
  """
  print('|--- Starting Gold Layer ---|')
  spark = create_spark_session()
  
  try:
    silver_df = spark.read.json(input_path)
    
    if silver_df.isEmpty():
      print("Warning: Silver layer is empty.")
      return

    # Write Daily Stats - Partitioned by Date
    print("Processing: Daily Source Stats...")
    stats_df = generate_source_stats(silver_df)
    stats_df.write.mode("overwrite").partitionBy("date").parquet(f"{output_dir}/daily_source_stats")

    # Write Keyword Trends
    print("Processing: Keyword Trends...")
    keywords_df = generate_top_keywords(silver_df)
    keywords_df.write.mode("overwrite").parquet(f"{output_dir}/keyword_trends")
    
    print("\n--- TOP 10 TRENDING KEYWORDS ---")
    keywords_df.show()
            
  finally:
    spark.stop()

def main():
  """
  Entry point for the news ETL pipeline that resolves local paths and triggers all layers.
  """
  root = pathlib.Path(__file__).parent.parent.resolve()

  # '../data/bronze/news_traffic_raw.jsonl'
  bronze_output = str(root / 'data' / 'bronze' / 'news_traffic_raw.jsonl')

  # '../data/silver/news_traffic_cleaned.jsonl'
  silver_output = str(root / 'data' / 'silver' / 'news_traffic_cleaned.jsonl')

  # '../data/gold/'
  gold_output_dir = str(root / 'data' / 'gold')

  execute_bronze_pipeline(bronze_output)
  run_silver_layer(bronze_output, silver_output)
  run_gold_layer(silver_output, gold_output_dir)

if __name__ == "__main__":
  main()