import json
import os
from typing import List, Any

from extraction.models import BronzeRecord, SilverRecord

def save_to_jsonl(data: List[Any], filepath: str, mode: str = 'a') -> None:
  os.makedirs(os.path.dirname(filepath), exist_ok=True)

  with open(filepath, mode, encoding='utf-8') as f:
    for record in data:
      f.write(json.dumps(record.to_dict()) + '\n')
  
  print(f'Succesfully saved {len(data)} records to {filepath}')

def load_bronze_records(filepath: str) -> List[BronzeRecord]:
  records: List[BronzeRecord] = []

  try:
    with open(filepath, 'r', encoding='utf-8') as f:
      for line in f:
        if not line.strip():
          continue

        data = json.loads(line)

        records.append(BronzeRecord(**data))
  except FileNotFoundError:
    print(f'Warning: File {filepath} not found.')
    return []

  return records