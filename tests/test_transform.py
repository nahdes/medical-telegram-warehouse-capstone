from ai_agent.transform import extract_prices, extract_product_terms, parse_text_record


def test_extract_prices_simple():
    text = "Paracetamol 100 ETB and aspirin $5"
    prices = extract_prices(text)
    assert "100 ETB" in prices
    assert "$5" in prices


def test_extract_product_terms():
    txt = "New tablet and capsule available"
    terms = extract_product_terms(txt)
    assert "tablet" in terms
    assert "capsule" in terms
    assert "syrup" not in terms


def test_parse_text_record_combination():
    rec = {
        'message_id': '1',
        'channel_name': 'ch',
        'message_text': 'buy aspirin 50 ETB',
        'ocr_text': 'aspirin'
    }
    parsed = parse_text_record(rec)
    assert parsed['products'] == ['aspirin']
    assert '50 ETB' in parsed['prices']
    assert 'aspirin' in parsed['source_text']
