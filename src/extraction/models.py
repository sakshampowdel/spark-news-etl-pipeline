from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class BronzeRecord:
  article_url: str
  raw_html: str
  source: str
  ingestion_timestamp: str = datetime.now().isoformat()

  def to_dict(self):
    return asdict(self)