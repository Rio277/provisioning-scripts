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

# Enable loading of truncated images
ImageFile.LOAD_TRUNCATED_IMAGES = True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_processor.log'),
        logging.StreamHandler()  # Add console output
    ]
)
logger = logging.getLogger(__name__)

class ImageProcessor:
    def __init__(self, 
                 directory: str,
                 naming_pattern: str = r".*\.png$",
                 r2_endpoint: str = None,
                 r2_access_key: str = None,
                 r2_secret_key: str = None,
                 bucket_name: str = None,
                 jpg_quality: int = 85):
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
        """
        self.directory = Path(directory)
        self.naming_pattern = re.compile(naming_pattern, re.IGNORECASE)
        self.bucket_name = bucket_name
        self.jpg_quality = jpg_quality
        
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
                logger.info("R2 client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize R2 client: {e}")
                raise
        else:
            logger.warning("R2 credentials not provided. Set via environment variables or parameters.")
    
    def find_matching_images(self) -> List[Path]:
        """Find PNG images matching the naming convention"""
        if not self.directory.exists():
            logger.error(f"Directory does not exist: {self.directory}")
            return []
        
        matching_files = []
        for file_path in self.directory.iterdir():
            if file_path.is_file() and self.naming_pattern.match(file_path.name):
                matching_files.append(file_path)
                logger.info(f"Found matching file: {file_path.name}")
        
        logger.info(f"Found {len(matching_files)} matching PNG files")
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
                logger.info(f"Converted {png_path.name} to {jpg_path.name}")
                return jpg_path
                
        except Exception as e:
            logger.error(f"Failed to convert {png_path}: {e}")
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
        
        # Parse the filename format: {id}-{seed}_{sequence}_
        # Expected pattern: 1418510004060-890774523686991_00001_
        pattern = r'^(\d+)-(\d+)_\d+_'
        
        match = re.match(pattern, filename)
        
        if match:
            file_id = match.group(1)  # 1418510004060
            seed_value = match.group(2)  # 890774523686991
            
            processed_filename = f"{file_id}.jpg"
            metadata = {
                'seed': seed_value
            }
            
            logger.info(f"Processed filename: {file_path.name} -> {processed_filename} (seed: {seed_value})")
            return processed_filename, metadata
        else:
            # Fallback for non-matching filenames
            logger.warning(f"Filename doesn't match expected pattern: {filename}")
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
            logger.info(f"Adding metadata: {metadata}")
            print(f"📝 Adding metadata: {metadata}")
        else:
            logger.info("No metadata to add")
            print(f"📝 No metadata found for {file_path.name}")
        
        try:
            with open(file_path, 'rb') as file_data:
                self.s3_client.upload_fileobj(
                    file_data,
                    self.bucket_name,
                    object_key,
                    ExtraArgs=extra_args
                )
            logger.info(f"Successfully uploaded {file_path.name} to R2 as {object_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to upload {file_path.name} to R2: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error uploading {file_path.name}: {e}")
            return False
    
    def cleanup_files(self, png_path: Path, jpg_path: Path, keep_converted: bool = False):
        """Remove local files after successful upload"""
        try:
            if png_path.exists():
                png_path.unlink()
                logger.info(f"Removed original PNG: {png_path.name}")
            
            if not keep_converted and jpg_path.exists():
                jpg_path.unlink()
                logger.info(f"Removed converted JPG: {jpg_path.name}")
            elif keep_converted:
                logger.info(f"Kept converted JPG: {jpg_path.name}")
                
        except Exception as e:
            logger.error(f"Failed to cleanup files: {e}")
    
    def process_images(self, cleanup_on_success: bool = True, keep_converted: bool = False) -> dict:
        """Main processing function"""
        results = {
            'processed': 0,
            'converted': 0,
            'uploaded': 0,
            'cleaned': 0,
            'errors': []
        }
        
        matching_images = self.find_matching_images()
        results['processed'] = len(matching_images)
        
        for png_path in matching_images:
            try:
                print(f"Processing: {png_path.name}")
                
                # Convert PNG to JPG
                jpg_path = self.convert_png_to_jpg(png_path)
                if not jpg_path:
                    print(f"❌ Failed to convert: {png_path.name}")
                    results['errors'].append(f"Conversion failed: {png_path.name}")
                    continue
                
                results['converted'] += 1
                print(f"✓ Converted: {png_path.name} -> {jpg_path.name}")
                
                # Upload to R2 with processed filename and metadata
                if self.s3_client:
                    # Get metadata from the original PNG filename
                    _, metadata = self.process_filename_for_upload(png_path)
                    upload_success = self.upload_to_r2(jpg_path, metadata=metadata)
                    if not upload_success:
                        print(f"❌ Upload failed: {jpg_path.name}")
                        results['errors'].append(f"Upload failed: {jpg_path.name}")
                        # Clean up JPG file even if upload failed
                        if jpg_path.exists():
                            jpg_path.unlink()
                        continue
                    
                    results['uploaded'] += 1
                    print(f"✓ Uploaded: {jpg_path.name}")
                else:
                    # For dry run, show the processed filename that would be uploaded
                    processed_filename, _ = self.process_filename_for_upload(png_path)
                    _, metadata = self.process_filename_for_upload(png_path)
                    print(f"✓ Dry run - would upload as: {processed_filename}")
                    if metadata:
                        print(f"📝 Would add metadata: {metadata}")
                    results['uploaded'] += 1
                
                # Cleanup local files if upload was successful
                if cleanup_on_success:
                    self.cleanup_files(png_path, jpg_path, keep_converted)
                    results['cleaned'] += 1
                    if keep_converted:
                        print(f"✓ Removed PNG, kept JPG: {png_path.name}")
                    else:
                        print(f"✓ Cleaned up: {png_path.name}")
                    
            except Exception as e:
                error_msg = f"Error processing {png_path.name}: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Process PNG images and upload to R2')
    parser.add_argument('directory', help='Directory to scan for images')
    parser.add_argument('--pattern', default=r".*\.png$", 
                       help='Regex pattern for matching filenames (default: any PNG)')
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
    
    args = parser.parse_args()
    
    # Get credentials from environment if not provided as arguments
    r2_endpoint = args.r2_endpoint or os.getenv('R2_ENDPOINT')
    r2_access_key = args.r2_access_key or os.getenv('R2_ACCESS_KEY')
    r2_secret_key = args.r2_secret_key or os.getenv('R2_SECRET_KEY')
    
    if not args.dry_run and not all([r2_endpoint, r2_access_key, r2_secret_key]):
        logger.error("R2 credentials must be provided via arguments or environment variables:")
        logger.error("  --r2-endpoint or R2_ENDPOINT")
        logger.error("  --r2-access-key or R2_ACCESS_KEY")
        logger.error("  --r2-secret-key or R2_SECRET_KEY")
        return 1
    
    try:
        processor = ImageProcessor(
            directory=args.directory,
            naming_pattern=args.pattern,
            r2_endpoint=r2_endpoint if not args.dry_run else None,
            r2_access_key=r2_access_key if not args.dry_run else None,
            r2_secret_key=r2_secret_key if not args.dry_run else None,
            bucket_name=args.bucket,
            jpg_quality=args.quality
        )
        
        results = processor.process_images(cleanup_on_success=not args.no_cleanup, keep_converted=args.keep_converted)
        
        # Print summary to both console and log
        print("=" * 50)
        print("PROCESSING SUMMARY")
        print("=" * 50)
        print(f"Files processed: {results['processed']}")
        print(f"Successfully converted: {results['converted']}")
        print(f"Successfully uploaded: {results['uploaded']}")
        print(f"Files cleaned up: {results['cleaned']}")
        print(f"Errors: {len(results['errors'])}")
        
        logger.info("=" * 50)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Files processed: {results['processed']}")
        logger.info(f"Successfully converted: {results['converted']}")
        logger.info(f"Successfully uploaded: {results['uploaded']}")
        logger.info(f"Files cleaned up: {results['cleaned']}")
        logger.info(f"Errors: {len(results['errors'])}")
        
        if results['errors']:
            print("Errors encountered:")
            logger.error("Errors encountered:")
            for error in results['errors']:
                print(f"  - {error}")
                logger.error(f"  - {error}")
        
        return 0 if not results['errors'] else 1
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())