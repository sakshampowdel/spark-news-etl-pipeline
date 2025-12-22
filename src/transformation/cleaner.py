from bs4 import BeautifulSoup

from extraction.models import BronzeRecord, SilverRecord

def clean_reuters(soup: BeautifulSoup) -> dict:
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