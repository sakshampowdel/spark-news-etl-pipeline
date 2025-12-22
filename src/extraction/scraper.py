from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from typing import List

from extraction.models import BronzeRecord

def start_session(session: requests.Session, base_url: str, target_url: str) -> requests.Response:
  # Pretend to be a real user
  session.get(base_url, timeout=10) 
  
  # Now try the actual target URL
  response = session.get(target_url, timeout=10)

  # Raise exception if error
  response.raise_for_status()

  return response

def scrape_reuters(session: requests.Session) -> List[BronzeRecord]:
  """
  Scrape reuters website for US news articles

  Returns:
    List[BronzeRecord]: A list of each article as a BronzeRecord
  """
  response = start_session(session, 'https://www.reuters.com', 'https://www.reuters.com/world/us/')

  soup = BeautifulSoup(response.text, 'html.parser')

  # Grab all articles in the feed
  articles = soup.find_all('li', attrs={'data-testid':'FeedListItem'})

  results: List[BronzeRecord] = []
  for article in articles:
    # Grab the title div
    link_div = article.find(attrs={'data-testid':'TitleLink'})

    if link_div and link_div.has_attr('href'):
      # Transform to url
      article_url: str = urljoin('https://www.reuters.com', str(link_div.get('href')))

      record = BronzeRecord(article_url, str(article), 'Reuters')
      results.append(record)

  return results

def scrape_npr(session: requests.Session) -> List[BronzeRecord]:
  response = start_session(session, 'https://www.npr.org', 'https://www.npr.org/sections/politics')

  soup = BeautifulSoup(response.text, 'html.parser')

  # Grab all articles in the feed
  articles = soup.find_all('article', {'class':'item'})

  results: List[BronzeRecord] = []
  for article in articles:
    # Grab the href link holder
    link_a = article.find('a')

    if link_a and link_a.has_attr('href'):
      # Transform to url
      article_url: str = str(link_a.get('href'))

      record = BronzeRecord(article_url, str(article), 'NPR')
      results.append(record)

  return results

def scrape_article_to_bronze() -> List[BronzeRecord]:
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

  sources = [
    #scrape_reuters,
    scrape_npr
  ]

  results: List[BronzeRecord] = []

  for scrape_func in sources:
    try:
      print(f'Running scraper: {scrape_func.__name__}...')
      data = scrape_func(session)
      results.extend(data)
    except Exception as e:
      print(f'Error in {scrape_func.__name__}: {e}')
      continue
  
  return results
