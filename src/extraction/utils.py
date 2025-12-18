import json
import os
from typing import List

from extraction.models import BronzeRecord

def save_to_bronze(data: List[BronzeRecord], filepath: str) -> None:
  os.makedirs(os.path.dirname(filepath), exist_ok=True)

  with open(filepath, 'a', encoding='utf-8') as f:
    for record in data:
      f.write(json.dumps(record.to_dict()) + '\n')
  
  print(f'Succesfully saved {len(data)} records to {filepath}')