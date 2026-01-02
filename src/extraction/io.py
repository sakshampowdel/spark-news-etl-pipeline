import json
import logging
from typing import Generator

from extraction.models import BronzeRecord

logger = logging.getLogger('io')

def stream_bronze_records(filepath: str) -> Generator[BronzeRecord, None, None]:
  """
  Reads a JSONL file and yields BronzeRecord objects lazily.

  Encapsulates file I/O, JSON parsing, and model instantiation to provide 
  a clean stream for downstream transformation.

  Args:
    filepath (str): The path to the source .jsonl file.

  Yields:
    BronzeRecord: An instantiated data model for each valid line in the file.
  """
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
