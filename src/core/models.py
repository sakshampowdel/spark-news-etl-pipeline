from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class BronzeRecord:
  """
  Represents the raw data extracted from a source before cleaning.

  Attributes:
    article_url (str): The direct link to the news article.
    raw_html (str): The unparsed HTML fragment of the article feed item.
    source (str): The name of the publisher (e.g., 'Reuters').
    ingestion_timestamp (str): ISO 8601 formatted string of when the data was pulled.
  """
  article_url: str
  raw_html: str
  source: str
  ingestion_timestamp: str = datetime.now().isoformat()

  def to_dict(self):
    """Converts the dataclass instance to a dictionary for JSON serialization."""
    return asdict(self)

@dataclass
class SilverRecord:
  """
  Represents cleaned and validated news article data.

  Attributes:
    article_url (str): The validated absolute URL of the article.
    title (str): The cleaned headline, stripped of HTML tags and extra whitespace.
    teaser (str): A brief summary or first paragraph of the article.
    source (str): The origin of the article.
    ingestion_timestamp (str): Original timestamp carried over from the Bronze layer.
  """
  article_url: str
  title: str
  teaser: str
  source: str
  ingestion_timestamp: str = datetime.now().isoformat()

  def to_dict(self):
    """Converts the dataclass instance to a dictionary for JSON serialization."""
    return asdict(self)