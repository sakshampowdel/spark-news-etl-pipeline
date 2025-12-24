from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
from typing import List

from extraction.models import BronzeRecord

def start_session(session: requests.Session, base_url: str, target_url: str) -> requests.Response:
  """
  Initializes a session by visiting a base URL before requesting the target data.

  Args:
    session (requests.Session): The current shared session with custom headers.
    base_url (str): The homepage URL used to establish initial cookies/handshake.
    target_url (str): The specific news feed endpoint to scrape.

  Returns:
    requests.Response: The HTTP response from the target URL if successful.

  Raises:
    requests.exceptions.HTTPError: If the target_url returns a 4xx or 5xx status code.
    requests.exceptions.RequestException: For any other networking-related issues.
  """
  # Pretend to be a real user
  session.get(base_url, timeout=10) 
  
  # Now try the actual target URL
  response = session.get(target_url, timeout=10)

  # Raise exception if error
  response.raise_for_status()

  return response

def scrape_reuters(session: requests.Session) -> List[BronzeRecord]:
  """
  Scrapes the Reuters US World news feed for raw article data.

  Args:
    session (requests.Session): The active session used for requests.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
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
  """
  Scrapes the NPR Politics section for raw article data.

  Args:
    session (requests.Session): The active session used for requests.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
  """
  response = start_session(session, 'https://www.npr.org', 'https://www.npr.org/sections/politics')

  soup = BeautifulSoup(response.text, 'html.parser')

  # Grab all articles in the feed
  articles = soup.find_all('article', attrs={'class':'item'})

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

# TODO: Refactor to Playwright. Current BS4 method is blocked by WaPo
def scrape_wapo(session: requests.Session) -> List[BronzeRecord]:
  response = start_session(session, 'https://www.washingtonpost.com', 'https://www.washingtonpost.com/politics/')

  soup = BeautifulSoup(response.text, 'html.parser')

  # Grab all articles in the feed
  articles = soup.find_all('div', attrs={'data-feature-id':'homepage/story'})

  results: List[BronzeRecord] = []
  for article in articles:
    # Grab the href link holder
    link_a = article.find('a')

    if link_a and link_a.has_attr('href'):
      # Transform to url
      article_url: str = str(link_a.get('href'))

      record = BronzeRecord(article_url, str(article), 'The Washington Post')
      results.append(record)
  
  return results

def scrape_article_to_bronze() -> List[BronzeRecord]:
  """
  Orchestrates the extraction process across all defined news sources.

  This function configures a shared session with realistic browser headers and 
  iterates through all scraper functions to build a combined Bronze dataset.

  Returns:
    List[BronzeRecord]: A unified list of all raw records pulled from all sources.
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

  sources = [
    scrape_reuters,
    scrape_npr,
    #scrape_wapo
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
