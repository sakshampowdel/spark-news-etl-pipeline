import pathlib
import logging
import os

from pipelines.extraction.scraper import extract_to_bronze
from infrastructure.io import stream_bronze_records
from pipelines.transformation.cleaner import transform_to_silver
from pipelines.analysis.analyzer import create_spark_session, SILVER_SCHEMA, generate_top_keywords
from core.logging_utils import setup_logging

setup_logging()
logger = logging.getLogger('main')

def execute_bronze_pipeline(output_path: str) -> None:
  """
  Orchestrates the full extraction process from web sources to Bronze storage.

  Ensures the target directory exists and manages the file buffer during 
  the extraction phase.

  Args:
    output_path (str): The file path where raw records will be appended.

  Raises:
    RuntimeError: If the extraction process results in zero persisted records.
  """
  logger.info("Starting Bronze Layer execution...")

  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  with open(output_path, 'a', encoding='utf-8') as buffer:
    records_persisted: int = extract_to_bronze(buffer)

  if records_persisted == 0:
    logger.error("No records were extracted to Bronze!")
    raise RuntimeError("Bronze pipeline failed: Zero records saved.")

  logger.info(f"Bronze layer complete. Total records persisted: {records_persisted}.")

def execute_silver_pipeline(input_path: str, output_path: str) -> None:
  """
  Orchestrates the transformation of raw Bronze data into structured Silver.

  Streams records from the source path, applies transformation logic, and 
  persists the results to a clean output file.

  Args:
    input_path (str): The path to the source Bronze .jsonl file.
    output_path (str): The path where the cleaned Silver .jsonl will be saved.

  Raises:
    RuntimeError: If no records are successfully transformed and persisted.
  """
  logger.info("Starting Silver Layer execution...")

  os.makedirs(os.path.dirname(output_path), exist_ok=True)

  bronze_stream = stream_bronze_records(input_path)

  with open(output_path, 'w', encoding='utf-8') as buffer:
    records_persisted: int = transform_to_silver(bronze_stream, buffer)

  if records_persisted == 0:
    logger.error("No records were transformed to Silver!")
    raise RuntimeError("Silver pipeline failed: Zero records persisted.")
  
  logger.info(f"Silver layer complete. Total records persisted: {records_persisted}.")

def execute_gold_pipeline(input_path: str, output_path: str) -> None:
  """
    Handles the I/O: Reads Silver JSONL, runs analysis, writes Gold Parquet.
  """
  logger.info("Starting Gold Layer execution...")
  
  spark = create_spark_session()
  spark.sparkContext.setLogLevel("ERROR")
  
  try:
    silver_df = spark.read.schema(SILVER_SCHEMA).json(input_path)
    
    if silver_df.isEmpty():
      logger.warning("Silver layer is empty. Skipping analysis.")
      return

    logger.info("Processing Keyword Trends...")
    keywords_df = generate_top_keywords(silver_df)

    keywords_df.write.mode("overwrite").parquet(f"{output_path}/keyword_trends")
    
    print("\n--- TOP TRENDING KEYWORDS ---")
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
  GOLD_PATH = str(root / 'data' / 'gold')

  execute_bronze_pipeline(BRONZE_PATH)
  execute_silver_pipeline(BRONZE_PATH, SILVER_PATH)
  execute_gold_pipeline(SILVER_PATH, GOLD_PATH)

if __name__ == "__main__":
  main()