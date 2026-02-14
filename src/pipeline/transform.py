import re
from typing import List, Dict

# simple regex for prices (supports ETB birr, USD, numbers with separators)
PRICE_REGEX = re.compile(
    r"\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:ETB|birr|USD|US\$|\$)?\b",
    flags=re.IGNORECASE
)

# naive product vocabulary; in real systems this would come from a trained NER model
PRODUCT_TERMS = [
    'tablet', 'capsule', 'syrup', 'injection', 'ointment', 'cream',
    'bandage', 'mask', 'gloves', 'antibiotic', 'vitamin', 'aspirin',
    'paracetamol', 'ibuprofen', 'cough syrup'
]


def extract_prices(text: str) -> List[str]:
    """Return all price-like strings found in the input text."""
    return PRICE_REGEX.findall(text or "")


def extract_product_terms(text: str) -> List[str]:
    """Return any known product terms that appear in the text."""
    found = []
    if not text:
        return found
    lowered = text.lower()
    for term in PRODUCT_TERMS:
        if term in lowered:
            found.append(term)
    return found


def parse_text_record(record: Dict) -> Dict:
    """Given a dictionary with OCR/text fields, return structured parse.

    Expected keys: 'message_text' (optional), 'ocr_text' (optional)
    """
    full_text = []
    for key in ('message_text', 'ocr_text'):
        if record.get(key):
            full_text.append(record[key])
    combined = " | ".join(full_text)

    prices = extract_prices(combined)
    products = extract_product_terms(combined)
    return {
        'message_id': record.get('message_id'),
        'channel_name': record.get('channel_name'),
        'products': products,
        'prices': prices,
        'source_text': combined
    }


def process_yolo_ocr_file(csv_path: str) -> List[Dict]:
    """Load the YOLO output CSV and produce price/product extraction records."""
    import csv

    records = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = parse_text_record(row)
            if rec['products'] or rec['prices']:
                records.append(rec)
    return records
