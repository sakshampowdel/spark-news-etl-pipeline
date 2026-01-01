from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Browser
import requests
from urllib.parse import urljoin
from typing import List

from extraction.models import BronzeRecord

def start_light_session(session: requests.Session, base_url: str, target_url: str) -> requests.Response:
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

def start_heavy_session(browser: Browser, target_url: str):
  context = browser.new_context(
    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    viewport={'width': 1920, 'height': 1080}
  )

  page = context.new_page()

  page.set_extra_http_headers({
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"'
  })

  page.goto(target_url, wait_until='domcontentloaded')

  return context, page

def scrape_reuters(session: requests.Session) -> List[BronzeRecord]:
  """
  Scrapes the Reuters US World news feed for raw article data.

  Args:
    session (requests.Session): The active session used for requests.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
  """
  response = start_light_session(session, 'https://www.reuters.com', 'https://www.reuters.com/world/us/')

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
  response = start_light_session(session, 'https://www.npr.org', 'https://www.npr.org/sections/politics')

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

def scrape_wapo(browser: Browser) -> List[BronzeRecord]:
  context, page = start_heavy_session(browser, 'https://www.washingtonpost.com/politics/')

  # Grab all articles in the feed
  articles = page.locator('div[data-feature-id="homepage/story"]').all()

  results: List[BronzeRecord] = []
  for article in articles:
    html_fragment = article.inner_html()
    link_a = article.locator('a').last
    href = link_a.get_attribute('href')

    if href:
      record = BronzeRecord(href, html_fragment, 'The Washington Post')
      results.append(record)
  
  context.close()

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

  light_sources = [
    scrape_reuters,
    scrape_npr,
  ]

  heavy_sources = [
    scrape_wapo
  ]

  results: List[BronzeRecord] = []

  for scrape_func in light_sources:
    try:
      print(f'Running light scraper: {scrape_func.__name__}...')
      data = scrape_func(session)

      if data:
        results.extend(data)
        print(f'Successfully scraped {len(data)} records.')
      else:
        print(f'Warning: {scrape_func.__name__} returned 0 results!')
      
    except Exception as e:
      print(f'Error in {scrape_func.__name__}: {e}')
  

  with sync_playwright() as pw:
    browser = pw.chromium.launch(
      headless=True,
      args=[
        "--disable-http2",                        # Forces HTTP/1.1 to avoid the protocol error
        "--disable-blink-features=AutomationControlled", # Hides the 'navigator.webdriver' flag
        "--no-sandbox",                           # Required for Docker
        "--disable-dev-shm-usage"                 # Prevents memory crashes in containers
      ]
    )

    for scrape_func in heavy_sources:
      try:
        print(f'Running heavy scraper: {scrape_func.__name__}...')
        data = scrape_func(browser)

        if data:
          results.extend(data)
          print(f'Successfully scraped {len(data)} records.')
        else:
          print(f'Warning: {scrape_func.__name__} returned 0 results!')
        
      except Exception as e:
        print(f'Error in {scrape_func.__name__}: {e}')
    
  
  return results
