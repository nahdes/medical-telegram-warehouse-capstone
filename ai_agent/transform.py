"""Text parsing and NLP helpers."""

import re
from typing import List, Dict

PRICE_REGEX = re.compile(
    r"\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:ETB|birr|USD|US\$|\$)?\b",
    flags=re.IGNORECASE
)
PRODUCT_TERMS = [
    'tablet', 'capsule', 'syrup', 'injection', 'ointment', 'cream',
    'bandage', 'mask', 'gloves', 'antibiotic', 'vitamin', 'aspirin',
    'paracetamol', 'ibuprofen', 'cough syrup'
]


def extract_prices(text: str) -> List[str]:
    return PRICE_REGEX.findall(text or "")


def extract_product_terms(text: str) -> List[str]:
    found: List[str] = []
    if not text:
        return found
    lowered = text.lower()
    for term in PRODUCT_TERMS:
        if term in lowered:
            found.append(term)
    return found


def parse_text_record(record: Dict) -> Dict:
    combined = " | ".join(record.get(k, '') for k in ('message_text', 'ocr_text') if record.get(k))
    return {
        'message_id': record.get('message_id'),
        'channel_name': record.get('channel_name'),
        'products': extract_product_terms(combined),
        'prices': extract_prices(combined),
        'source_text': combined
    }


def process_yolo_ocr_file(csv_path: str) -> List[Dict]:
    import csv
    recs: List[Dict] = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = parse_text_record(row)
            if rec['products'] or rec['prices']:
                recs.append(rec)
    return recs
