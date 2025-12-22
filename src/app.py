from typing import List
import pathlib

from extraction.models import BronzeRecord, SilverRecord
from extraction.scraper import scrape_reuters
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
  bronze_records: List[BronzeRecord] = scrape_reuters()

  if not bronze_records:
    raise RuntimeError('Error scraping bronze records')

  save_to_jsonl(bronze_records, output_path, mode='a')

def run_silver_layer(intput_path: str, output_path: str) -> None:
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
  bronze_records: List[BronzeRecord] = load_bronze_records(intput_path)

  if not bronze_records:
    raise RuntimeError('Error grabbing bronze records')

  silver_records: List[SilverRecord] = (
    [transform_bronze_to_silver(record) for record in bronze_records]
  )
  
  save_to_jsonl(silver_records, output_path, mode='w')


def main():
  root = pathlib.Path(__file__).parent.parent.resolve()

  # '../data/bronze/reuters_us.jsonl'
  bronze_output = str(root / 'data' / 'bronze' / 'reuters_us.jsonl')

  # '../data/silver/reuters_us.jsonl'
  silver_output = str(root / 'data' / 'silver' / 'reuters_us.jsonl')

  #run_bronze_layer(bronze_output)
  run_silver_layer(bronze_output, silver_output)

if __name__ == "__main__":
  main()