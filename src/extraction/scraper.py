from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from typing import List

from extraction.models import BronzeRecord


def scrape_reuters() -> List[BronzeRecord]:
  """
  Returns a list of BronzeRecords in JSON format from Reuters
  """
  # Create a session object
  session = requests.Session()
  
  # Pretend to be a real agent
  session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/',
    'DNT': '1'
  })

  try:
    # Pretend to be a real user
    session.get('https://www.reuters.com', timeout=10) 
    
    # Now try the actual target URL
    response = session.get('https://www.reuters.com/world/us/', timeout=10)
    response.raise_for_status()

  except requests.RequestException as e:
    print(f'Extraction failed with error: {e}')
    return []

  soup = BeautifulSoup(response.text, 'html.parser')

  # Grab all articles in the feed
  articles = soup.find_all('li', attrs={'data-testid':'FeedListItem'})

  results: List = []
  for article in articles:
    # Grab the title div
    link_div = article.find(attrs={'data-testid':'TitleLink'})

    if link_div and link_div.has_attr('href'):
      # Transform to url
      article_url: str = urljoin('https://www.reuters.com', str(link_div.get('href')))

      record = BronzeRecord(article_url, str(article), 'Reuters')
      results.append(record)

  return results

