"""
Simple OCR helper using pytesseract (Tesseract must be installed separately).
"""

import logging
from pathlib import Path
from typing import Optional

try:
    import pytesseract
    from PIL import Image
except ImportError as e:
    logging.error("OCR dependencies not installed: %s", e)
    raise

logger = logging.getLogger(__name__)


def extract_text_from_region(image_path: Path, bbox: list) -> str:
    """Crop the given bounding-box region and run OCR on it.

    Args:
        image_path: Path to the source image.
        bbox: [x1, y1, x2, y2] coordinates.
    Returns:
        The text recognised in that region (empty string if none).
    """
    try:
        with Image.open(image_path) as img:
            x1, y1, x2, y2 = [int(coord) for coord in bbox]
            cropped = img.crop((x1, y1, x2, y2))
            text = pytesseract.image_to_string(cropped, lang='eng')
            return text.strip()
    except Exception as e:
        logger.warning(f"OCR failed for {image_path} bbox={bbox}: {e}")
        return ""


def extract_text_bulk(image_path: Path, bboxes: list) -> list:
    """Return a list of OCR text strings for each bounding box."""
    texts = []
    for bbox in bboxes:
        texts.append(extract_text_from_region(image_path, bbox))
    return texts
