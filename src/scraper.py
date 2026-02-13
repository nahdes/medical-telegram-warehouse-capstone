"""
Telegram Data Scraper for Ethiopian Medical Businesses
Extracts messages and media from public Telegram channels
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChannelPrivateError

# Load environment variables
load_dotenv()

# Configuration
API_ID = os.getenv('api_id')
API_HASH = os.getenv('api_hash')
SESSION_NAME = 'telegram_scraper_session'

# Directory structure
BASE_DIR = Path('data')
RAW_DIR = BASE_DIR / 'raw'
IMAGES_DIR = RAW_DIR / 'images'
MESSAGES_DIR = RAW_DIR / 'telegram_messages'
LOGS_DIR = Path('logs')

# Telegram channels to scrape
CHANNELS = [
    'https://t.me/CheMed123',
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    # Add more channels from https://et.tgstat.com/medicine
]


class TelegramScraper:
    """Scraper for extracting data from Telegram channels"""
    
    def __init__(self, api_id: str, api_hash: str, session_name: str):
        """Initialize the Telegram client"""
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.logger = self._setup_logging()
        self._ensure_directories()
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        log_filename = LOGS_DIR / f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger(__name__)
    
    def _ensure_directories(self):
        """Create necessary directory structure"""
        IMAGES_DIR.mkdir(parents=True, exist_ok=True)
        MESSAGES_DIR.mkdir(parents=True, exist_ok=True)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("Directory structure created successfully")
    
    def _extract_channel_name(self, channel_url: str) -> str:
        """Extract channel name from URL"""
        return channel_url.split('/')[-1].replace('@', '')
    
    async def download_media(self, message, channel_name: str, message_id: int) -> Optional[str]:
        """Download media from message"""
        try:
            if not message.media:
                return None
            
            # Check if it's a photo or document with image
            if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
                # Create channel-specific image directory
                channel_img_dir = IMAGES_DIR / channel_name
                channel_img_dir.mkdir(parents=True, exist_ok=True)
                
                # Define image path
                image_path = channel_img_dir / f"{message_id}.jpg"
                
                # Download the media
                await self.client.download_media(message, file=str(image_path))
                
                self.logger.info(f"Downloaded image: {image_path}")
                return str(image_path.relative_to(BASE_DIR))
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error downloading media for message {message_id}: {str(e)}")
            return None
    
    def _extract_message_data(self, message, channel_name: str, image_path: Optional[str]) -> Dict:
        """Extract relevant data from a message"""
        return {
            'message_id': message.id,
            'channel_name': channel_name,
            'message_date': message.date.isoformat() if message.date else None,
            'message_text': message.message if message.message else '',
            'has_media': bool(message.media),
            'image_path': image_path,
            'views': message.views if hasattr(message, 'views') else 0,
            'forwards': message.forwards if hasattr(message, 'forwards') else 0,
            'is_reply': message.is_reply,
            'reply_to_msg_id': message.reply_to_msg_id if message.is_reply else None,
        }
    
    async def scrape_channel(self, channel_url: str, limit: Optional[int] = None) -> List[Dict]:
        """Scrape messages from a single channel"""
        channel_name = self._extract_channel_name(channel_url)
        messages_data = []
        
        try:
            self.logger.info(f"Starting to scrape channel: {channel_name}")
            
            # Get the channel entity
            entity = await self.client.get_entity(channel_url)
            
            # Iterate through messages
            message_count = 0
            async for message in self.client.iter_messages(entity, limit=limit):
                try:
                    # Download media if present
                    image_path = None
                    if message.media:
                        image_path = await self.download_media(message, channel_name, message.id)
                    
                    # Extract message data
                    message_data = self._extract_message_data(message, channel_name, image_path)
                    messages_data.append(message_data)
                    
                    message_count += 1
                    
                    if message_count % 10 == 0:
                        self.logger.info(f"Scraped {message_count} messages from {channel_name}")
                    
                    # Small delay to avoid rate limiting
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"Error processing message {message.id} from {channel_name}: {str(e)}")
                    continue
            
            self.logger.info(f"Completed scraping {channel_name}: {message_count} messages")
            return messages_data
            
        except ChannelPrivateError:
            self.logger.error(f"Channel {channel_name} is private or doesn't exist")
            return []
        except FloodWaitError as e:
            self.logger.warning(f"Rate limited. Need to wait {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await self.scrape_channel(channel_url, limit)
        except Exception as e:
            self.logger.error(f"Error scraping channel {channel_name}: {str(e)}")
            return []
    
    def save_to_data_lake(self, messages_data: List[Dict], channel_name: str):
        """Save scraped data to the data lake with partitioning"""
        if not messages_data:
            self.logger.warning(f"No data to save for {channel_name}")
            return
        
        # Get current date for partitioning
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Create partitioned directory
        partition_dir = MESSAGES_DIR / current_date
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as JSON
        output_file = partition_dir / f"{channel_name}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(messages_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Saved {len(messages_data)} messages to {output_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving data to {output_file}: {str(e)}")
    
    async def scrape_all_channels(self, limit_per_channel: Optional[int] = None):
        """Scrape all configured channels"""
        self.logger.info("Starting scraping process for all channels")
        
        scraping_summary = {
            'start_time': datetime.now().isoformat(),
            'channels': []
        }
        
        for channel_url in CHANNELS:
            channel_name = self._extract_channel_name(channel_url)
            
            try:
                # Scrape channel
                messages_data = await self.scrape_channel(channel_url, limit=limit_per_channel)
                
                # Save to data lake
                self.save_to_data_lake(messages_data, channel_name)
                
                # Update summary
                scraping_summary['channels'].append({
                    'channel_name': channel_name,
                    'channel_url': channel_url,
                    'messages_scraped': len(messages_data),
                    'status': 'success'
                })
                
            except Exception as e:
                self.logger.error(f"Failed to scrape {channel_name}: {str(e)}")
                scraping_summary['channels'].append({
                    'channel_name': channel_name,
                    'channel_url': channel_url,
                    'messages_scraped': 0,
                    'status': 'failed',
                    'error': str(e)
                })
        
        scraping_summary['end_time'] = datetime.now().isoformat()
        
        # Save scraping summary
        summary_file = LOGS_DIR / f'scraping_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(scraping_summary, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Scraping completed. Summary saved to {summary_file}")
        
        return scraping_summary
    
    async def run(self, limit_per_channel: Optional[int] = None):
        """Main execution method"""
        try:
            # Start the client
            await self.client.start()
            self.logger.info("Telegram client started successfully")
            
            # Scrape all channels
            summary = await self.scrape_all_channels(limit_per_channel)
            
            # Print summary
            print("\n" + "="*50)
            print("SCRAPING SUMMARY")
            print("="*50)
            for channel in summary['channels']:
                print(f"\nChannel: {channel['channel_name']}")
                print(f"Status: {channel['status']}")
                print(f"Messages: {channel['messages_scraped']}")
            print("="*50 + "\n")
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}")
            raise
        finally:
            await self.client.disconnect()
            self.logger.info("Telegram client disconnected")


async def main():
    """Entry point for the scraper"""
    # Validate API credentials
    if not API_ID or not API_HASH:
        raise ValueError("API_ID and API_HASH must be set in .env file")
    
    # Create scraper instance
    scraper = TelegramScraper(API_ID, API_HASH, SESSION_NAME)
    
    # Run scraper (limit to 100 messages per channel for testing, set to None for all messages)
    await scraper.run(limit_per_channel=100)


if __name__ == '__main__':
    asyncio.run(main())