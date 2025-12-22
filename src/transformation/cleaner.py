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
  
  title: str = title_span.get_text()

  description_p = soup.find(attrs={'data-testid':'Description'})

  if not description_p:
    raise RuntimeError('Did not find valid description!')
  
  description: str = description_p.get_text()

  return {
    'title': title,
    'teaser': description
  }

def clean_npr(soup: BeautifulSoup) -> dict:
  title_h2 = soup.find('h2')

  if not title_h2:
    raise RuntimeError('NPR: Did not find valid title! (h2)')
  
  title_a = title_h2.find('a')

  if not title_a:
    raise RuntimeError('NPR: Did not find valid title! (a)')
  
  title: str = title_a.get_text()

  teaser_p = soup.find('p')

  if not teaser_p:
    raise RuntimeError('NPR: Did not find valid teaser! (p)')
  
  teaser_a = teaser_p.find('a')

  if not teaser_a:
    raise RuntimeError('NPR: Did not find valid teaser! (a)')
  
  teaser: str = teaser_a.get_text()

  return {
    'title': title,
    'teaser': teaser
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
    'Reuters': clean_reuters,
    'NPR': clean_npr
  }

  clean_func = parsers.get(bronze.source)

  if not clean_func:
    raise RuntimeError('Did not find valid source!')

  data: dict[str, str] = clean_func(soup)

  return SilverRecord(
    article_url=bronze.article_url,
    title=data['title'],
    teaser=data['teaser'],
    source=bronze.source,
    ingestion_timestamp=bronze.ingestion_timestamp
  )