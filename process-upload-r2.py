#!/usr/bin/env python3
"""
Image Processing Script for R2 Upload
Processes PNG images matching naming convention, converts to JPG, uploads to R2, and cleans up local files.
"""

import os
import re
import logging
import time
from pathlib import Path
from typing import List, Optional, Tuple
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from PIL import Image, ImageFile
import argparse
import sqlite3
from datetime import datetime
import json
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Enable loading of truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_processor.log')
        # Only log to file, not console
    ]
)
logger = logging.getLogger(__name__)

class UploadTracker:
    def __init__(self, db_path: str = "upload_tracker.db"):
        # Always place database in the same directory as the script
        script_dir = Path(__file__).parent.absolute()
        if not os.path.isabs(db_path):
            self.db_path = script_dir / db_path
        else:
            self.db_path = Path(db_path)
        self.db_lock = Lock()
        self.init_db()
    
    def init_db(self):
        """Initialize SQLite database with upload tracking table"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS upload_status (
                    card_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.commit()
    
    def is_uploaded(self, card_id: str) -> bool:
        """Check if card_id has already been uploaded"""
        with self.db_lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT status FROM upload_status WHERE card_id = ? AND status = 'uploaded'",
                    (card_id,)
                )
                return cursor.fetchone() is not None
    
    def mark_uploaded(self, card_id: str):
        """Mark card_id as uploaded"""
        with self.db_lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO upload_status (card_id, status, timestamp)
                    VALUES (?, 'uploaded', ?)
                """, (card_id, datetime.now().isoformat()))
                conn.commit()
    
    def batch_mark_uploaded(self, card_ids: List[str]):
        """Mark multiple card_ids as uploaded in a single transaction"""
        if not card_ids:
            return
        
        with self.db_lock:
            with sqlite3.connect(self.db_path) as conn:
                timestamp = datetime.now().isoformat()
                data = [(card_id, 'uploaded', timestamp) for card_id in card_ids]
                conn.executemany("""
                    INSERT OR REPLACE INTO upload_status (card_id, status, timestamp)
                    VALUES (?, ?, ?)
                """, data)
                conn.commit()
    
    def batch_check_uploaded(self, card_ids: List[str]) -> set:
        """Check multiple card_ids and return set of already uploaded ones"""
        if not card_ids:
            return set()
        
        with self.db_lock:
            with sqlite3.connect(self.db_path) as conn:
                placeholders = ','.join('?' * len(card_ids))
                cursor = conn.execute(
                    f"SELECT card_id FROM upload_status WHERE card_id IN ({placeholders}) AND status = 'uploaded'",
                    card_ids
                )
                return {row[0] for row in cursor.fetchall()}

class ImageProcessor:
    def __init__(self, 
                 directory: str,
                 naming_pattern: str = r".*\.png$",
                 r2_endpoint: str = None,
                 r2_access_key: str = None,
                 r2_secret_key: str = None,
                 bucket_name: str = None,
                 jpg_quality: int = 85,
                 track_uploads: bool = True,
                 max_workers: int = 4):
        """
        Initialize the ImageProcessor
        
        Args:
            directory: Directory to scan for images
            naming_pattern: Regex pattern for matching filenames (default: any PNG file)
            r2_endpoint: Cloudflare R2 endpoint URL
            r2_access_key: R2 access key
            r2_secret_key: R2 secret key
            bucket_name: R2 bucket name
            jpg_quality: JPEG quality (1-100, default: 85)
            track_uploads: Whether to track uploaded files to avoid duplicates
            max_workers: Maximum number of concurrent workers (default: 4)
        """
        self.directory = Path(directory)
        self.naming_pattern = re.compile(naming_pattern, re.IGNORECASE)
        self.bucket_name = bucket_name
        self.jpg_quality = jpg_quality
        self.max_workers = max_workers
        self.tracker = UploadTracker() if track_uploads else None
        
        # Initialize R2 client
        self.s3_client = None
        if all([r2_endpoint, r2_access_key, r2_secret_key]):
            try:
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=r2_endpoint,
                    aws_access_key_id=r2_access_key,
                    aws_secret_access_key=r2_secret_key,
                    region_name='auto'  # R2 uses 'auto' region
                )
            except Exception as e:
                logger.error(f"Failed to initialize R2 client: {e}")
                raise
    
    def find_matching_images(self) -> List[Path]:
        """Find PNG images matching the naming convention"""
        if not self.directory.exists():
            return []
        
        matching_files = []
        for file_path in self.directory.iterdir():
            if file_path.is_file() and self.naming_pattern.match(file_path.name):
                matching_files.append(file_path)
        
        return matching_files
    
    def convert_png_to_jpg(self, png_path: Path) -> Optional[Path]:
        """Convert PNG to JPG format"""
        try:
            # Get processed filename for the JPG
            processed_filename, _ = self.process_filename_for_upload(png_path)
            jpg_path = png_path.parent / processed_filename
            
            # Open and convert image
            with Image.open(png_path) as img:
                # Convert RGBA to RGB if necessary
                if img.mode in ('RGBA', 'LA', 'P'):
                    # Create white background
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                    img = background
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Save as JPG
                img.save(jpg_path, 'JPEG', quality=self.jpg_quality, optimize=True)
                return jpg_path
                
        except Exception as e:
            return None
    
    def process_filename_for_upload(self, file_path: Path) -> tuple[str, dict]:
        """
        Process filename for your specific format: 1418510004060-890774523686991_00001_.png
        
        Args:
            file_path: Local file path
            
        Returns:
            Tuple of (processed_filename, metadata_dict)
        """
        filename = file_path.stem  # Get filename without extension
        
        # Parse the filename format: pregen_{id}-{seed}_{sequence}_
        # Expected pattern: pregen_1418510004060-890774523686991_00001_
        pattern = r'^pregen_(\d+)-(\d+)_\d+_$'
        
        match = re.match(pattern, filename)
        
        if match:
            file_id = match.group(1)  # 1418510004060
            seed_value = match.group(2)  # 890774523686991
            
            processed_filename = f"{file_id}.jpg"
            metadata = {
                'seed': seed_value
            }
            
            return processed_filename, metadata
        else:
            # Fallback for non-matching filenames
            processed_filename = f"{filename}.jpg"
            metadata = {}
            return processed_filename, metadata
    
    def upload_to_r2(self, file_path: Path, object_key: Optional[str] = None, metadata: Optional[dict] = None) -> bool:
        """Upload file to R2 bucket"""
        if not self.s3_client:
            logger.error("R2 client not initialized")
            return False
        
        if not object_key:
            object_key, file_metadata = self.process_filename_for_upload(file_path)
            # Use file metadata if no metadata was passed
            if not metadata:
                metadata = file_metadata
        
        # Prepare upload arguments
        extra_args = {
            'ContentType': 'image/jpeg'
        }
        
        # Add metadata if provided
        if metadata:
            extra_args['Metadata'] = metadata
        
        try:
            # Read full file content into memory first
            with open(file_path, 'rb') as file_data:
                file_bytes = file_data.read()
            
            # Upload with complete file bytes in body
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=object_key,
                Body=file_bytes,
                ContentType='image/jpeg',
                CacheControl='public, max-age=31536000, immutable',
                ContentEncoding='identity',
                Metadata=metadata or {}
            )
            return True
            
        except ClientError as e:
            return False
        except Exception as e:
            return False
    
    def cleanup_files(self, png_path: Path, jpg_path: Path, keep_converted: bool = False):
        """Remove local files after successful upload"""
        try:
            if png_path.exists():
                png_path.unlink()
            
            if not keep_converted and jpg_path.exists():
                jpg_path.unlink()
                
        except Exception as e:
            pass
    
    def process_single_image(self, png_path: Path, cleanup_on_success: bool = True, keep_converted: bool = False) -> dict:
        """Process a single image (for use in concurrent processing)"""
        result = {
            'processed': 0,
            'converted': 0,
            'uploaded': 0,
            'cleaned': 0,
            'errors': [],
            'card_id': None
        }
        
        try:
            # Cache filename processing to avoid redundant calls
            processed_filename, metadata = self.process_filename_for_upload(png_path)
            card_id = processed_filename.replace('.jpg', '')
            result['card_id'] = card_id
            
            # Check if already uploaded (thread-safe)
            if self.tracker and self.tracker.is_uploaded(card_id):
                message = f"⏩ {png_path.name} - already uploaded (skipped)"
                print(message)
                logger.info(message)
                return result
            
            result['processed'] = 1
            
            # Convert PNG to JPG
            jpg_path = self.convert_png_to_jpg(png_path)
            if not jpg_path:
                message = f"❌ {png_path.name} - conversion failed"
                print(message)
                logger.info(message)
                result['errors'].append(f"Conversion failed: {png_path.name}")
                return result
            
            result['converted'] = 1
            
            # Upload to R2 with processed filename and metadata
            if self.s3_client:
                upload_success = self.upload_to_r2(jpg_path, processed_filename, metadata)
                if not upload_success:
                    message = f"❌ {png_path.name} - upload failed"
                    print(message)
                    logger.info(message)
                    result['errors'].append(f"Upload failed: {jpg_path.name}")
                    # Clean up JPG file even if upload failed
                    if jpg_path.exists():
                        jpg_path.unlink()
                    return result
                
                result['uploaded'] = 1
                status = "uploaded"
            else:
                # For dry run
                result['uploaded'] = 1
                status = "dry-run"
            
            # Cleanup local files if upload was successful
            if cleanup_on_success:
                self.cleanup_files(png_path, jpg_path, keep_converted)
                result['cleaned'] = 1
                cleanup_status = "PNG removed" if keep_converted else "cleaned"
            else:
                cleanup_status = "kept"
            
            message = f"✓ {png_path.name} -> {jpg_path.name} ({status}, {cleanup_status})"
            print(message)
            logger.info(message)
            
        except Exception as e:
            error_msg = f"Error processing {png_path.name}: {e}"
            result['errors'].append(error_msg)
            message = f"❌ {png_path.name} - error: {e}"
            print(message)
            logger.info(message)
        
        return result
    
    def process_images(self, cleanup_on_success: bool = True, keep_converted: bool = False) -> dict:
        """Main processing function with concurrent processing"""
        results = {
            'processed': 0,
            'converted': 0,
            'uploaded': 0,
            'cleaned': 0,
            'errors': []
        }
        
        matching_images = self.find_matching_images()
        if not matching_images:
            return results
        
        print(f"Processing {len(matching_images)} images with {self.max_workers} concurrent workers...")
        
        # For batch database operations
        successfully_uploaded_cards = []
        
        # Process images concurrently
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_path = {
                executor.submit(self.process_single_image, png_path, cleanup_on_success, keep_converted): png_path 
                for png_path in matching_images
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_path):
                try:
                    result = future.result()
                    
                    # Aggregate results
                    results['processed'] += result['processed']
                    results['converted'] += result['converted']
                    results['uploaded'] += result['uploaded']
                    results['cleaned'] += result['cleaned']
                    results['errors'].extend(result['errors'])
                    
                    # Collect card IDs for batch database update
                    if result['uploaded'] > 0 and result['card_id'] and self.tracker and self.s3_client:
                        successfully_uploaded_cards.append(result['card_id'])
                        
                except Exception as e:
                    png_path = future_to_path[future]
                    error_msg = f"Future execution error for {png_path.name}: {e}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
        
        # Batch update database for all successfully uploaded files
        if successfully_uploaded_cards and self.tracker:
            try:
                self.tracker.batch_mark_uploaded(successfully_uploaded_cards)
                logger.info(f"Batch updated {len(successfully_uploaded_cards)} upload records")
            except Exception as e:
                logger.error(f"Failed to batch update upload records: {e}")
        
        return results

def load_config(config_path: str) -> dict:
    """Load R2 credentials from config file (JSON or INI format)"""
    config_file = Path(config_path)
    if not config_file.exists():
        return {}
    
    try:
        # Try JSON format first
        if config_path.endswith('.json'):
            with open(config_path, 'r') as f:
                config = json.load(f)
                return {
                    'r2_endpoint': config.get('r2_endpoint'),
                    'r2_access_key': config.get('r2_access_key'),
                    'r2_secret_key': config.get('r2_secret_key'),
                    'bucket_name': config.get('bucket_name')
                }
        
        # Try INI format
        else:
            config = configparser.ConfigParser()
            config.read(config_path)
            r2_section = config['r2'] if 'r2' in config else config['DEFAULT']
            return {
                'r2_endpoint': r2_section.get('endpoint'),
                'r2_access_key': r2_section.get('access_key'),
                'r2_secret_key': r2_section.get('secret_key'),
                'bucket_name': r2_section.get('bucket_name')
            }
    
    except Exception as e:
        logger.error(f"Failed to load config file {config_path}: {e}")
        return {}

def main():
    parser = argparse.ArgumentParser(description='Process PNG images and upload to R2')
    parser.add_argument('directory', help='Directory to scan for images')
    parser.add_argument('--pattern', default=r"pregen_\d+-\d+_\d+_\.png$", 
                       help='Regex pattern for matching filenames (default: pregen_{id}-{seed}_{seq}_.png)')
    parser.add_argument('--r2-endpoint', help='R2 endpoint URL')
    parser.add_argument('--r2-access-key', help='R2 access key')
    parser.add_argument('--r2-secret-key', help='R2 secret key')
    parser.add_argument('--bucket', required=True, help='R2 bucket name')
    parser.add_argument('--quality', type=int, default=85, help='JPEG quality (1-100)')
    parser.add_argument('--no-cleanup', action='store_true', 
                       help='Keep local files after upload')
    parser.add_argument('--dry-run', action='store_true',
                       help='Process files locally without uploading to R2')
    parser.add_argument('--keep-converted', action='store_true',
                       help='Keep converted JPG files, only remove original PNG files')
    parser.add_argument('--config', help='Path to config file (JSON or INI format)')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Maximum number of concurrent workers (default: 4)')
    
    args = parser.parse_args()
    
    # Load config file if provided
    config = {}
    if args.config:
        config = load_config(args.config)
    
    # Get credentials from config file, environment variables, or arguments (in that order of priority)
    r2_endpoint = args.r2_endpoint or config.get('r2_endpoint') or os.getenv('R2_ENDPOINT')
    r2_access_key = args.r2_access_key or config.get('r2_access_key') or os.getenv('R2_ACCESS_KEY')
    r2_secret_key = args.r2_secret_key or config.get('r2_secret_key') or os.getenv('R2_SECRET_KEY')
    bucket_name = args.bucket or config.get('bucket_name')
    
    if not args.dry_run and not all([r2_endpoint, r2_access_key, r2_secret_key]):
        logger.error("R2 credentials must be provided via config file, arguments, or environment variables:")
        logger.error("  --config config.json")
        logger.error("  --r2-endpoint or R2_ENDPOINT")
        logger.error("  --r2-access-key or R2_ACCESS_KEY")
        logger.error("  --r2-secret-key or R2_SECRET_KEY")
        return 1
    
    if not bucket_name:
        logger.error("Bucket name must be provided via --bucket argument or config file")
        return 1
    
    try:
        processor = ImageProcessor(
            directory=args.directory,
            naming_pattern=args.pattern,
            r2_endpoint=r2_endpoint if not args.dry_run else None,
            r2_access_key=r2_access_key if not args.dry_run else None,
            r2_secret_key=r2_secret_key if not args.dry_run else None,
            bucket_name=bucket_name,
            jpg_quality=args.quality,
            track_uploads=True,
            max_workers=args.max_workers
        )
        
        results = processor.process_images(cleanup_on_success=not args.no_cleanup, keep_converted=args.keep_converted)
        
        # Print summary
        summary = f"Summary: {results['processed']} processed, {results['converted']} converted, {results['uploaded']} uploaded, {results['cleaned']} cleaned, {len(results['errors'])} errors"
        print(summary)
        logger.info(summary)
        
        if results['errors']:
            for error in results['errors']:
                logger.error(error)
        
        return 0 if not results['errors'] else 1
        
    except Exception as e:
        print(f"Script failed: {e}")
        logger.error(f"Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())