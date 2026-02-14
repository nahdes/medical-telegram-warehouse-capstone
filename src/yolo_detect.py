"""
YOLO Object Detection for Telegram Images
Detects objects in downloaded images and categorizes them
"""

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
from dotenv import load_dotenv

try:
    from ultralytics import YOLO
    from PIL import Image
    # OCR helper will be imported lazily later (optional dependency)
except ImportError:
    print("ERROR: Required packages not installed!")
    print("Run: pip install ultralytics pillow")
    exit(1)

# Load environment variables
load_dotenv()

# Configuration
IMAGES_DIR = Path('data/raw/images')
OUTPUT_DIR = Path('data/processed')
LOGS_DIR = Path('logs')
MODEL_NAME = 'yolov8n.pt'  # Nano model for efficiency

# Setup logging
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / f'yolo_detection_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ImageDetector:
    """YOLO-based object detection for product images"""
    
    # Class names relevant to medical/pharmaceutical products
    PRODUCT_CLASSES = {'bottle', 'cup', 'bowl', 'vase', 'cell phone'}
    PERSON_CLASS = 'person'
    
    def __init__(self, model_name: str = MODEL_NAME):
        """Initialize YOLO model"""
        logger.info(f"Loading YOLO model: {model_name}")
        try:
            self.model = YOLO(model_name)
            logger.info("YOLO model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {e}")
            raise
    
    def detect_objects(self, image_path: Path) -> List[Dict]:
        """
        Run YOLO detection on an image
        Returns list of detected objects with confidence scores
        """
        try:
            # Run inference
            results = self.model(str(image_path), verbose=False)
            
            detections = []
            for result in results:
                boxes = result.boxes
                for box in boxes:
                    # Get class name and confidence
                    class_id = int(box.cls[0])
                    class_name = self.model.names[class_id]
                    confidence = float(box.conf[0])
                    
                    detections.append({
                        'class': class_name,
                        'confidence': confidence,
                        'bbox': box.xyxy[0].tolist()  # Bounding box coordinates
                    })
            
            return detections
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {e}")
            return []
    
    def categorize_image(self, detections: List[Dict]) -> Tuple[str, List[str], float]:
        """
        Categorize image based on detected objects
        
        Returns:
            - category: promotional/product_display/lifestyle/other
            - detected_classes: list of detected class names
            - max_confidence: highest confidence score
        """
        if not detections:
            return 'other', [], 0.0
        
        # Extract detected classes and confidences
        detected_classes = [d['class'] for d in detections]
        confidences = [d['confidence'] for d in detections]
        max_confidence = max(confidences) if confidences else 0.0
        
        # Check for specific object types
        has_person = self.PERSON_CLASS in detected_classes
        has_product = any(cls in self.PRODUCT_CLASSES for cls in detected_classes)
        
        # Categorization logic
        if has_person and has_product:
            category = 'promotional'  # Person holding/showing product
        elif has_product and not has_person:
            category = 'product_display'  # Just the product
        elif has_person and not has_product:
            category = 'lifestyle'  # Person without product
        else:
            category = 'other'  # Neither detected
        
        return category, detected_classes, max_confidence
    
    def process_all_images(self) -> List[Dict]:
        """
        Process all images in the data/raw/images directory
        Returns list of detection results
        """
        results = []
        
        if not IMAGES_DIR.exists():
            logger.warning(f"Images directory not found: {IMAGES_DIR}")
            return results
        
        # Find all image files
        image_files = []
        for channel_dir in IMAGES_DIR.iterdir():
            if channel_dir.is_dir():
                for img_file in channel_dir.glob('*.jpg'):
                    image_files.append((channel_dir.name, img_file))
        
        if not image_files:
            logger.warning("No images found to process")
            return results
        
        logger.info(f"Found {len(image_files)} images to process")
        
        # Process each image
        # try to import OCR helpers (optional)
        try:
            from ocr import extract_text_from_region
        except ImportError:
            extract_text_from_region = None  # OCR will be skipped

        for idx, (channel_name, image_path) in enumerate(image_files, 1):
            try:
                # Extract message_id from filename
                message_id = image_path.stem  # Filename without extension

                logger.info(f"[{idx}/{len(image_files)}] Processing: {channel_name}/{message_id}")

                # Run detection
                detections = self.detect_objects(image_path)

                # Optionally run OCR on each bounding box
                ocr_texts = []
                if extract_text_from_region and detections:
                    for d in detections:
                        bbox = d.get('bbox', [])
                        text = extract_text_from_region(image_path, bbox)
                        ocr_texts.append(text)

                # Categorize image
                category, detected_classes, max_confidence = self.categorize_image(detections)

                # Store results
                result = {
                    'message_id': message_id,
                    'channel_name': channel_name,
                    'image_path': str(image_path.relative_to(Path('.'))),
                    'category': category,
                    'detected_objects': ', '.join(detected_classes),
                    'num_detections': len(detections),
                    'max_confidence': round(max_confidence, 4),
                    'detections_json': str(detections),  # Full detection details
                    'ocr_text': ' | '.join(ocr_texts) if ocr_texts else ''
                }

                results.append(result)

                logger.info(f"  Category: {category} | Objects: {len(detections)} | "
                          f"Max Confidence: {max_confidence:.2f}")

            except Exception as e:
                logger.error(f"Error processing {image_path}: {e}")
                continue
        
        return results
    
    def save_results(self, results: List[Dict], output_file: Path):
        """Save detection results to CSV"""
        if not results:
            logger.warning("No results to save")
            return
        
        try:
            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to CSV
            fieldnames = [
                'message_id', 'channel_name', 'image_path', 'category',
                'detected_objects', 'num_detections', 'max_confidence',
                'detections_json', 'ocr_text'
            ]
            
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            logger.info(f"Results saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            raise
    
    def print_summary(self, results: List[Dict]):
        """Print summary statistics"""
        if not results:
            return
        
        # Category distribution
        categories = {}
        for r in results:
            cat = r['category']
            categories[cat] = categories.get(cat, 0) + 1
        
        # Channel distribution
        channels = {}
        for r in results:
            ch = r['channel_name']
            channels[ch] = channels.get(ch, 0) + 1
        
        print("\n" + "="*60)
        print("YOLO DETECTION SUMMARY")
        print("="*60)
        print(f"\nTotal Images Processed: {len(results)}")
        
        print(f"\nCategory Distribution:")
        for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            pct = (count / len(results)) * 100
            print(f"  {cat:20} {count:4} ({pct:5.1f}%)")
        
        print(f"\nImages by Channel:")
        for ch, count in sorted(channels.items(), key=lambda x: x[1], reverse=True):
            print(f"  {ch:20} {count:4}")
        
        # Average detections
        avg_detections = sum(r['num_detections'] for r in results) / len(results)
        avg_confidence = sum(r['max_confidence'] for r in results) / len(results)
        
        print(f"\nAverage Detections per Image: {avg_detections:.1f}")
        print(f"Average Max Confidence: {avg_confidence:.3f}")
        print("="*60 + "\n")


def main():
    """Main execution"""
    print("="*60)
    print("YOLO OBJECT DETECTION - TELEGRAM IMAGES")
    print("="*60)
    
    # Initialize detector
    detector = ImageDetector()
    
    # Process all images
    results = detector.process_all_images()
    
    if not results:
        print("\n⚠️  No images were processed!")
        print("Make sure you have run the scraper first (Task 1)")
        return
    
    # Save results
    output_file = OUTPUT_DIR / 'yolo_detections.csv'
    detector.save_results(results, output_file)
    
    # Print summary
    detector.print_summary(results)
    
    print(f"✅ Detection complete!")
    print(f"   Results saved to: {output_file}")
    print(f"   Next: Load results into warehouse with dbt")


if __name__ == '__main__':
    main()