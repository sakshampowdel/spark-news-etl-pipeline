from bs4 import BeautifulSoup

from extraction.models import BronzeRecord, SilverRecord

def clean_reuters(soup: BeautifulSoup) -> dict:
  """
  Clean the raw HTML data for each article preview for the Reuters website.

  Args:
    soup (BeautifulSoup): The data structure representing the parsed raw HTML data.

  Raises:
    RuntimeError: If there is no valid title for the article.
    RuntimeError: If there is no valid description for the article.

  Returns:
    dict: title and teaser as strings.
  """
  title_span = soup.find(attrs={'data-testid':'TitleHeading'})

  if not title_span:
    raise RuntimeError('Did not find valid title!')
  
  title = title_span.get_text()

  description_p = soup.find(attrs={'data-testid':'Description'})

  if not description_p:
    raise RuntimeError('Did not find valid description!')
  
  description = description_p.get_text()

  return {
    'title': title,
    'teaser': description
  }

def transform_bronze_to_silver(bronze: BronzeRecord) -> SilverRecord:
  """
  Transforms a BronzeRecord object to a SilverRecord object.

  Args:
    bronze (BronzeRecord): The BronzeRecord object to be transformed.

  Raises:
    RuntimeError: If a valid source was not specified.

  Returns:
    SilverRecord: A new object after cleaning up the BronzeRecord.
  """
  soup = BeautifulSoup(bronze.raw_html, 'html.parser')

  parsers = {
    'Reuters': clean_reuters
  }

  clean_func = parsers.get(bronze.source)

  if not clean_func:
    raise RuntimeError('Did not find valid source!')

  data = clean_func(soup)

  return SilverRecord(
    article_url=bronze.article_url,
    title=data['title'],
    teaser=data['teaser'],
    source=bronze.source,
    ingestion_timestamp=bronze.ingestion_timestamp
  )