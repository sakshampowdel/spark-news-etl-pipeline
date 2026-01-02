import requests
import logging
import json
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Browser
from urllib.parse import urljoin
from typing import Generator, TextIO
from concurrent.futures import ThreadPoolExecutor

from extraction.models import BronzeRecord

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("extraction")

def init_requests_session(session: requests.Session, base_url: str, target_url: str) -> requests.Response:
  session.get(base_url, timeout=10) 
  response = session.get(target_url, timeout=10)
  response.raise_for_status()

  return response

def init_playwright_session(browser: Browser, target_url: str):
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

def stream_reuters_feed(session: requests.Session) -> Generator[BronzeRecord, None, None]:
  response = init_requests_session(session, 'https://www.reuters.com', 'https://www.reuters.com/world/us/')
  soup = BeautifulSoup(response.text, 'html.parser')
  articles = soup.find_all('li', attrs={'data-testid':'FeedListItem'})

  for article in articles:
    link_div = article.find(attrs={'data-testid':'TitleLink'})
    if link_div and link_div.has_attr('href'):
      article_url = urljoin('https://www.reuters.com', str(link_div.get('href')))
      yield BronzeRecord(article_url, str(article), 'Reuters')

def stream_npr_feed(session: requests.Session) -> Generator[BronzeRecord, None, None]:
  response = init_requests_session(session, 'https://www.npr.org', 'https://www.npr.org/sections/politics')
  soup = BeautifulSoup(response.text, 'html.parser')
  articles = soup.find_all('article', attrs={'class':'item'})

  for article in articles:
    link_a = article.find('a')
    if link_a and link_a.has_attr('href'):
      article_url = str(link_a.get('href'))
      yield BronzeRecord(article_url, str(article), 'NPR')

def stream_wapo_feed(browser: Browser) -> Generator[BronzeRecord, None, None]:
  context, page = init_playwright_session(browser, 'https://www.washingtonpost.com/politics/')
  
  try:
    articles = page.locator('div[data-feature-id="homepage/story"]').all()

    for article in articles:
      try:
        html_fragment = article.inner_html()
        link_a = article.locator('a').last
        href = link_a.get_attribute('href')

        if href:
          yield BronzeRecord(href, html_fragment, 'The Washington Post')
      except Exception as e:
        logger.warning(f"Skipping individual WaPo article due to error: {e}")
  finally:
    context.close()

def extract_to_bronze(buffer: TextIO) -> int:
  total_count = 0
  light_sources = [
    stream_reuters_feed,
    stream_npr_feed
  ]
  heavy_sources = [
    stream_wapo_feed
  ]

  with requests.Session() as session:
    session.headers.update({
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Referer': 'https://www.google.com/',
      'DNT': '1'
    })
    
    logger.info("Starting parallel extraction for light sources...")
    with ThreadPoolExecutor(max_workers=len(light_sources)) as executor:
      future_to_func = {executor.submit(src, session): src.__name__ for src in light_sources}
      
      for future in future_to_func:
        source_name = future_to_func[future]
        try:
          for record in future.result():
            buffer.write(json.dumps(record.to_dict()) + '\n')
            total_count += 1
          logger.info(f"{source_name}: Streamed successfully.")
        except Exception as e:
          logger.error(f"{source_name} failed: {e}")

  logger.info("Starting extraction for heavy sources...")
  with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, args=['--no-sandbox'])
    
    for source_func in heavy_sources:
      try:
        for record in source_func(browser):
          buffer.write(json.dumps(record.to_dict()) + '\n')
          total_count += 1
        logger.info(f"{source_func.__name__}: Streamed successfully.")
      except Exception as e:
        logger.error(f"{source_func.__name__} failed: {e}")
    
  return total_count
