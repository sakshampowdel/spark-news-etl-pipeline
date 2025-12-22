from typing import List
import pathlib

from extraction.models import BronzeRecord, SilverRecord
from extraction.scraper import scrape_article_to_bronze
from extraction.utils import save_to_jsonl, load_bronze_records
from transformation.cleaner import transform_bronze_to_silver

def run_bronze_layer(output_path: str) -> None:
  """
  Orchestrates the scraping of news articles into structured Bronze records.
  
  Args:
    output_path: The file path where the .jsonl data will be saved.
      
  Raises:
    RuntimeError: If no records are created scraping the articles.
  """
  print('|--- Starting Bronze Layer ---|')
  bronze_records: List[BronzeRecord] = scrape_article_to_bronze()

  if not bronze_records:
    raise RuntimeError('Error scraping bronze records')

  save_to_jsonl(bronze_records, output_path, mode='a')

def run_silver_layer(input_path: str, output_path: str) -> None:
  """
  Orchestrates the transformation of raw Bronze HTML records into 
  structured Silver records.
  
  Args:
    input_path: The file path to the raw .jsonl bronze data.
    output_path: The file path where the cleaned .jsonl data will be saved.
      
  Raises:
    RuntimeError: If no records are found in the bronze source.
  """
  print('|--- Starting Silver Layer ---|')
  bronze_records: List[BronzeRecord] = load_bronze_records(input_path)

  if not bronze_records:
    raise RuntimeError('Error grabbing bronze records')

  unique_map: dict[str, BronzeRecord] = {record.article_url: record for record in bronze_records}
  unique_records: List[BronzeRecord] = list(unique_map.values())

  silver_records: List[SilverRecord] = (
    [transform_bronze_to_silver(record) for record in unique_records]
  )
  
  save_to_jsonl(silver_records, output_path, mode='w')

def main():
  root = pathlib.Path(__file__).parent.parent.resolve()

  # '../data/bronze/news_traffic_raw.jsonl'
  bronze_output = str(root / 'data' / 'bronze' / 'news_traffic_raw.jsonl')

  # '../data/silver/news_traffic_cleaned.jsonl'
  silver_output = str(root / 'data' / 'silver' / 'news_traffic_cleaned.jsonl')

  run_bronze_layer(bronze_output)
  run_silver_layer(bronze_output, silver_output)

if __name__ == "__main__":
  main()