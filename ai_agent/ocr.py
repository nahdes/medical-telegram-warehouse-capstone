"""OCR utilities for the AI agent."""

from pathlib import Path
import logging

try:
    import pytesseract
    from PIL import Image
except ImportError:  # pragma: no cover
    pytesseract = None  # type: ignore
    Image = None  # type: ignore

logger = logging.getLogger(__name__)


def extract_text_from_region(image_path: Path, bbox: list) -> str:
    """Crop region and perform OCR. Returns trimmed string."""
    if pytesseract is None or Image is None:
        logger.warning("OCR libraries not available")
        return ""
    try:
        with Image.open(image_path) as img:
            x1, y1, x2, y2 = [int(c) for c in bbox]
            cropped = img.crop((x1, y1, x2, y2))
            return pytesseract.image_to_string(cropped, lang='eng').strip()
    except Exception as e:  # pragma: no cover
        logger.error(f"OCR error: {e}")
        return ""


def extract_text_bulk(image_path: Path, bboxes: list) -> list[str]:
    return [extract_text_from_region(image_path, bbox) for bbox in bboxes]
