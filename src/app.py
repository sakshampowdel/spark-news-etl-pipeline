from extraction.scraper import scrape_reuters
from extraction.utils import save_to_bronze

def main():
  print("Starting Reuters Scraper...")
  raw_records = scrape_reuters()

  if not raw_records:
    print("Error")
    return

  save_to_bronze(raw_records, '../data/bronze/reuters_us.jsonl')

if __name__ == "__main__":
  main()