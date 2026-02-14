"""Vision-related functionality for the AI agent."""

from __future__ import annotations

import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional

try:
    from ultralytics import YOLO
    from PIL import Image
except ImportError:  # pragma: no cover
    YOLO = None
    Image = None


logger = logging.getLogger(__name__)


# constants
MODEL_NAME_DEFAULT = "yolov8n.pt"
IMAGES_DIR = Path('data/raw/images')
OUTPUT_DIR = Path('data/processed')


class DetectorConfig:
    """Configuration for an image detector."""

    def __init__(self, model_name: str = MODEL_NAME_DEFAULT):
        self.model_name = model_name


class ImageDetector:
    """YOLO-based object detection for product images."""

    PRODUCT_CLASSES = {'bottle', 'cup', 'bowl', 'vase', 'cell phone'}
    PERSON_CLASS = 'person'

    def __init__(self, config: DetectorConfig | None = None):
        config = config or DetectorConfig()
        model_name = config.model_name
        logger.info(f"Loading YOLO model: {model_name}")
        if YOLO is None:
            raise ImportError("ultralytics package is not available")
        self.model = YOLO(model_name)
        logger.info("YOLO model loaded successfully")

    def detect_objects(self, image_path: Path) -> List[Dict]:
        """Run YOLO on a file and return detections (with confidence)."""
        if self.model is None:  # type: ignore
            return []
        results = self.model(str(image_path), verbose=False)
        detections: List[Dict] = []
        for result in results:
            for box in result.boxes:
                class_id = int(box.cls[0])
                class_name = self.model.names[class_id]
                confidence = float(box.conf[0])
                detections.append({
                    'class': class_name,
                    'confidence': confidence,
                    'bbox': box.xyxy[0].tolist()
                })
        return detections

    def categorize_image(self, detections: List[Dict]) -> Tuple[str, List[str], float]:
        """Map detection list to a high-level category."""
        if not detections:
            return 'other', [], 0.0
        detected = [d['class'] for d in detections]
        confidences = [d['confidence'] for d in detections]
        max_conf = max(confidences) if confidences else 0.0
        has_person = self.PERSON_CLASS in detected
        has_product = any(cls in self.PRODUCT_CLASSES for cls in detected)
        if has_person and has_product:
            category = 'promotional'
        elif has_product and not has_person:
            category = 'product_display'
        elif has_person and not has_product:
            category = 'lifestyle'
        else:
            category = 'other'
        return category, detected, max_conf

    def process_all_images(self) -> List[Dict]:
        """Walk the images directory, detect and optionally OCR."""
        from ai_agent.ocr import extract_text_from_region

        results: List[Dict] = []
        if not IMAGES_DIR.exists():
            logger.warning(f"Images directory not found: {IMAGES_DIR}")
            return results
        files: List[Tuple[str, Path]] = []
        for channel_dir in IMAGES_DIR.iterdir():
            if channel_dir.is_dir():
                for img in channel_dir.glob('*.jpg'):
                    files.append((channel_dir.name, img))
        for idx, (channel, img_path) in enumerate(files, 1):
            message_id = img_path.stem
            logger.info(f"[{idx}/{len(files)}] {channel}/{message_id}")
            dets = self.detect_objects(img_path)
            ocr_texts: List[str] = []
            if dets:
                for d in dets:
                    bbox = d.get('bbox', [])
                    text = extract_text_from_region(img_path, bbox)
                    ocr_texts.append(text)
            cat, classes, maxc = self.categorize_image(dets)
            results.append({
                'message_id': message_id,
                'channel_name': channel,
                'image_path': str(img_path.relative_to(Path('.'))),
                'category': cat,
                'detected_objects': ', '.join(classes),
                'num_detections': len(dets),
                'max_confidence': round(maxc, 4),
                'detections_json': str(dets),
                'ocr_text': ' | '.join(ocr_texts) if ocr_texts else ''
            })
        return results

    def save_results(self, results: List[Dict], output_file: Path) -> None:
        """Dump results list to CSV in output directory."""
        import csv
        if not results:
            logger.warning("Nothing to save")
            return
        output_file.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            'message_id', 'channel_name', 'image_path', 'category',
            'detected_objects', 'num_detections', 'max_confidence',
            'detections_json', 'ocr_text'
        ]
        with output_file.open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        logger.info(f"Saved to {output_file}")
