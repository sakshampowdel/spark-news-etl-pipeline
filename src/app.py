import pathlib
import logging
import os

from extraction.scraper import extract_to_bronze
from extraction.io import stream_bronze_records
from transformation.cleaner import transform_to_silver
from transformation.analyzer import create_spark_session, generate_source_stats, generate_top_keywords

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("main")

def execute_bronze_pipeline(output_path: str) -> None:
  logger.info("Starting Bronze Layer execution...")

  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  with open(output_path, 'a', encoding='utf-8') as buffer:
    records_persisted: int = extract_to_bronze(buffer)

  if records_persisted == 0:
    logger.error("No records were extracted to Bronze!")
    raise RuntimeError("Bronze pipeline failed: Zero records saved.")

  logger.info(f"Bronze layer complete. Total records persisted: {records_persisted}.")

def execute_silver_pipeline(input_path: str, output_path: str) -> None:
  logger.info('Starting Silver Layer...')

  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  bronze_stream = stream_bronze_records(input_path)

  with open(output_path, 'w', encoding='utf-8') as buffer:
    records_persisted: int = transform_to_silver(bronze_stream, buffer)

  if records_persisted == 0:
    logger.error("No records were transformed to Silver!")
    raise RuntimeError("Silver pipeline failed: Zero records persisted.")
  
  logger.info(f"Silver layer complete. Total records persisted: {records_persisted}.")

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
  BRONZE_PATH = str(root / 'data' / 'bronze' / 'news_traffic_raw.jsonl')

  # '../data/silver/news_traffic_cleaned.jsonl'
  SILVER_PATH = str(root / 'data' / 'silver' / 'news_traffic_cleaned.jsonl')

  # '../data/gold/'
  gold_output_dir = str(root / 'data' / 'gold')

  execute_bronze_pipeline(BRONZE_PATH)
  execute_silver_pipeline(BRONZE_PATH, SILVER_PATH)
  run_gold_layer(SILVER_PATH, gold_output_dir)

if __name__ == "__main__":
  main()