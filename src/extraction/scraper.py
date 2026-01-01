import requests
import logging
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Browser
from urllib.parse import urljoin
from typing import List, Generator
from concurrent.futures import ThreadPoolExecutor


from extraction.models import BronzeRecord

# Configure logging format: Timestamp - Name - Level - Message
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("extraction")

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
  """
  Initializes a Playwright browser context with stealth headers and navigates to the target.

  Args:
    browser (Browser): The launched Playwright browser instance.
    target_url (str): The specific news feed endpoint to scrape.

  Returns:
    Tuple[BrowserContext, Page]: The active browser context and the loaded page object.
  """
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

  page.route("**/*.{png,jpg,jpeg,svg,css,woff2}", lambda route: route.abort())

  page.goto(target_url, wait_until='domcontentloaded')

  return context, page

def scrape_reuters(session: requests.Session) -> Generator[BronzeRecord, None, None]:
  """
  Scrapes the Reuters US World news feed for raw article data.

  Args:
    session (requests.Session): The active session used for requests.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
  """
  response = start_light_session(session, 'https://www.reuters.com', 'https://www.reuters.com/world/us/')
  soup = BeautifulSoup(response.text, 'html.parser')
  articles = soup.find_all('li', attrs={'data-testid':'FeedListItem'})

  for article in articles:
    link_div = article.find(attrs={'data-testid':'TitleLink'})

    if link_div and link_div.has_attr('href'):
      article_url: str = urljoin('https://www.reuters.com', str(link_div.get('href')))

      yield BronzeRecord(article_url, str(article), 'Reuters')

def scrape_npr(session: requests.Session) -> Generator[BronzeRecord, None, None]:
  """
  Scrapes the NPR Politics section for raw article data.

  Args:
    session (requests.Session): The active session used for requests.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
  """
  response = start_light_session(session, 'https://www.npr.org', 'https://www.npr.org/sections/politics')
  soup = BeautifulSoup(response.text, 'html.parser')
  articles = soup.find_all('article', attrs={'class':'item'})

  for article in articles:
    link_a = article.find('a')

    if link_a and link_a.has_attr('href'):
      article_url: str = str(link_a.get('href'))

      yield BronzeRecord(article_url, str(article), 'NPR')

def scrape_wapo(browser: Browser) -> Generator[BronzeRecord, None, None]:
  """
  Scrapes The Washington Post Politics section for raw article data using Playwright.

  Args:
    browser (Browser): The active Playwright browser instance.

  Returns:
    List[BronzeRecord]: A list of objects containing the article URL and raw HTML fragment.
  """
  context, page = start_heavy_session(browser, 'https://www.washingtonpost.com/politics/')
  articles = page.locator('div[data-feature-id="homepage/story"]').all()

  for article in articles:
    html_fragment = article.inner_html()
    link_a = article.locator('a').last
    href = link_a.get_attribute('href')

    if href:
      yield BronzeRecord(href, html_fragment, 'The Washington Post')
  
  context.close()

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

  logger.info("Initializing parallel extraction for light sources...")

  with ThreadPoolExecutor(max_workers=len(light_sources)) as executor:
    future_to_func = {executor.submit(func, session): func.__name__ for func in light_sources}
        
    for future in future_to_func:
      func_name = future_to_func[future]
      try:
        generator = future.result()
        count = 0

        for record in generator:
          results.append(record)
          count += 1

        logger.info(f"{func_name}: Successfully yielded {count} records.")
      except Exception as e:
        logger.error(f"{func_name}: Failed with error: {str(e)}.")
  
  with sync_playwright() as pw:
    browser = pw.chromium.launch(
      headless=True,
      args=[
        '--disable-http2',
        '--no-sandbox'
      ]
    )

    for scrape_func in heavy_sources:
      logger.info(f"Running heavy scraper: {scrape_func.__name__}...")
      try:
        generator = scrape_func(browser)
        count = 0

        for record in generator:
          results.append(record)
          count += 1

        logger.info(f"{scrape_func.__name__}: Successfully yielded {count} records.")
      except Exception as e:
        logger.error(f"{scrape_func.__name__}: Failed with error: {str(e)}.")
    
  return results
