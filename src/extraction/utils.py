import json
import os
import logging
from typing import List, Any

from extraction.models import BronzeRecord, SilverRecord

logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('utils')

def save_to_jsonl(data: List[Any], filepath: str, mode: str = 'a') -> None:
  """
  Serializes a list of objects to a JSON Lines file.

  Args:
    data (List[Any]): A list of record objects that implement a .to_dict() method.
    filepath (str): The location to save the output file to.
    mode (str, optional): The mode of the file opener. Defaults to 'a'.
  """
  os.makedirs(os.path.dirname(filepath), exist_ok=True)

  with open(filepath, mode, encoding='utf-8') as f:
    for record in data:
      f.write(json.dumps(record.to_dict()) + '\n')
  
  logger.info(f"Successfully saved {len(data)} record to {filepath}.")

def load_bronze_records(filepath: str) -> List[BronzeRecord]:
  """
  Reads a jsonl file representation of a BronzeRecord object.

  Args:
    filepath (str): The location of the jsonl file.

  Returns:
    List[BronzeRecord]: The list of BronzeRecord objects read from the jsonl file or empty if file not found.
  """
  records: List[BronzeRecord] = []

  try:
    with open(filepath, 'r', encoding='utf-8') as f:
      for line in f:
        if not line.strip():
          continue

        data = json.loads(line)

        records.append(BronzeRecord(**data))
  except FileNotFoundError:
    logger.warning(f"Warning: File {filepath} not found.")
    return []

  return records