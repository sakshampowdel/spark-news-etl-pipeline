import logging
import json
from typing import Generator, TextIO
from bs4 import BeautifulSoup

from extraction.models import BronzeRecord, SilverRecord

logger = logging.getLogger('silver.transform')

def parse_reuters_html(soup: BeautifulSoup) -> dict[str, str]:
  """
  Extracts article metadata from raw Reuters HTML fragments.

  Args:
    soup (BeautifulSoup): The parsed HTML content of the article list item.

  Returns:
    dict[str, str]: A dictionary containing the 'title' and 'teaser'.

  Raises:
    ValueError: If required data-testid elements are missing from the HTML.
  """
  title_span = soup.find(attrs={'data-testid':'TitleHeading'})
  if not title_span:
    raise ValueError("Reuters: Missing 'TitleHeading' span.")

  description_p = soup.find(attrs={'data-testid':'Description'})
  if not description_p:
    raise ValueError("Reuters: Missing 'Description' paragraph.")
  
  return {
    'title': title_span.get_text(strip=True),
    'teaser': description_p.get_text(strip=True)
  }

def parse_npr_html(soup: BeautifulSoup) -> dict[str, str]:
  """
  Extracts article metadata from raw NPR HTML fragments.

  Args:
    soup (BeautifulSoup): The parsed HTML content of the article list item.

  Returns:
    dict[str, str]: A dictionary containing the 'title' and 'teaser'.

  Raises:
    ValueError: If the title or teaser elements cannot be located.
  """
  title_h2 = soup.find('h2', attrs={'class': 'title'})
  if not title_h2:
    raise ValueError("NPR: Missing 'title' h2.")
  
  title_a = title_h2.find('a')
  if not title_a:
    raise ValueError("NPR: Missing anchor tag in title h2.")

  teaser_p = soup.find('p', attrs={'class': 'teaser'})
  if not teaser_p:
    raise ValueError("NPR: Missing 'teaser' paragraph.")
  
  teaser_a = teaser_p.find('a')
  teaser_text = teaser_a.get_text(strip=True) if teaser_a else teaser_p.get_text(strip=True)
  
  return {
    'title': title_a.get_text(strip=True),
    'teaser': teaser_text
  }

def parse_wapo_html(soup: BeautifulSoup) -> dict[str, str]:
  """
  Extracts article metadata from raw Washington Post HTML fragments.

  Args:
    soup (BeautifulSoup): The parsed HTML content of the article list item.

  Returns:
    dict[str, str]: A dictionary containing the 'title' and 'teaser'.

  Raises:
    ValueError: If the card-title or teaser paragraph is missing.
  """
  title_h3 = soup.find('h3', attrs={'data-testid':'card-title'})
  if not title_h3:
    raise ValueError("The Washington Post: Missing 'card-title' h3.")

  teaser_p = soup.find('p')
  if not teaser_p:
    raise ValueError("The Washington Post: Missing teaser paragraph.")

  return {
    'title': title_h3.get_text(strip=True),
    'teaser': teaser_p.get_text(strip=True)
  }

def map_to_silver(bronze: BronzeRecord) -> SilverRecord:
  """
  Maps a raw BronzeRecord to a SilverRecord using source-specific parsers.

  Args:
    bronze (BronzeRecord): The raw record fetched from the Bronze layer.

  Returns:
    SilverRecord: The validated and structured record.

  Raises:
    KeyError: If the record source does not have a registered parser.
  """
  soup = BeautifulSoup(bronze.raw_html, 'html.parser')

  parsers = {
    'Reuters': parse_reuters_html,
    'NPR': parse_npr_html,
    'The Washington Post': parse_wapo_html
  }

  parser_func = parsers.get(bronze.source)
  if not parser_func:
    raise KeyError(f"Unsupported source: {bronze.source}")

  data = parser_func(soup)

  return SilverRecord(
    article_url=bronze.article_url,
    title=data['title'],
    teaser=data['teaser'],
    source=bronze.source,
    ingestion_timestamp=bronze.ingestion_timestamp
  )

def transform_to_silver(bronze_stream: Generator[BronzeRecord, None, None], buffer: TextIO) -> int:
  """
  Orchestrates the transformation of a Bronze data stream into structured Silver.

  Handles deduplication, invokes appropriate parsers, and persists valid 
  records to the output buffer while logging parsing failures.

  Args:
    bronze_stream (Generator): A stream of BronzeRecord objects to process.
    buffer (TextIO): The file-like object where Silver records are persisted.

  Returns:
    int: The total count of records successfully transformed and persisted.
  """
  seen_urls = set()
  processed_count = 0

  for record in bronze_stream:
    if record.article_url in seen_urls:
      continue

    try:
      silver = map_to_silver(record)
      buffer.write(json.dumps(silver.to_dict()) + '\n')
      seen_urls.add(record.article_url)
      processed_count += 1
    except (ValueError, KeyError) as e:
      logger.warning(f"Skipping record {record.article_url} from {record.source}: {e}")
    except Exception as e:
      logger.error(f"Unexpected error processing {record.article_url}: {e}")
  
  return processed_count