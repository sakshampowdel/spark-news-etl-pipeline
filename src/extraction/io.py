import json
import os
import logging
from typing import Generator, List, Any
from extraction.models import BronzeRecord

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('io')

def stream_bronze_records(filepath: str) -> Generator[BronzeRecord, None, None]:
  try:
    with open(filepath, 'r', encoding='utf-8') as buffer:
      for line in buffer:
        line = line.strip()
        if not line:
          continue

        try:
          data = json.loads(line)
          yield BronzeRecord(**data)
        except (json.JSONDecodeError, TypeError) as e:
          logger.warning(f"Skipping malformed JSON line in {filepath}: {e}")
        except Exception as e:
          logger.error(f"Unexpected error occurred: {e}")
  except FileNotFoundError:
    logger.warning(f"Warning: File {filepath} not found.")
