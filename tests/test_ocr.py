from ai_agent.ocr import extract_text_from_region, extract_text_bulk
from pathlib import Path

def test_ocr_no_libs(monkeypatch):
    # simulate missing pytesseract
    monkeypatch.setattr('ai_agent.ocr.pytesseract', None)
    text = extract_text_from_region(Path('nonexistent.jpg'), [0,0,10,10])
    assert text == ''


# cannot run real OCR without image file
