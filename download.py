#!/usr/bin/env python3
"""
YouTube Download Script v2
Downloads audio from YouTube channels using cookies-from-browser authentication.
Simpler and more reliable than username/password authentication.
"""

import yt_dlp
import os
import concurrent.futures
import logging
from datetime import datetime
import csv
from google.cloud import storage
import google.api_core.retry
import google.api_core.client_options
import threading
from pathlib import Path

# Configuration
def get_channel_urls():
    with open('/tmp/manifest.txt', 'r') as f:
        return [line.strip() for line in f.readlines()]

CHANNEL_URLS = get_channel_urls()

DOWNLOAD_DIRECTORY = "podcasts"
MAX_WORKERS = 4

# GCS Configuration
GCS_BUCKET_NAME = "multichannel-podcasts"
GCS_PREFIX = "raw_audio"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CSV tracking file
CSV_FILE = 'download_status.csv'
CSV_HEADERS = ['timestamp', 'url', 'filename', 'status', 'duration_seconds', 'error_message']

class DownloadCounters:
    """Thread-safe counters for download statistics."""
    def __init__(self):
        self.downloaded = 0
        self.uploaded = 0
        self.failed = 0
        self.lock = threading.Lock()

    def increment_downloaded(self):
        with self.lock:
            self.downloaded += 1

    def increment_uploaded(self):
        with self.lock:
            self.uploaded += 1

    def increment_failed(self):
        with self.lock:
            self.failed += 1

def write_csv_entry(url, filename, status, duration_seconds, error_message=""):
    """Writes a download status entry to the CSV file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = [timestamp, url, filename, status, duration_seconds, error_message]
    
    file_exists = os.path.exists(CSV_FILE)
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(CSV_HEADERS)
        writer.writerow(row)

def initialize_gcs_client():
    """Initialize Google Cloud Storage client with timeout configuration."""
    try:
        client_options = google.api_core.client_options.ClientOptions(
            api_endpoint="https://storage.googleapis.com",
            api_audience="https://storage.googleapis.com"
        )
        storage_client = storage.Client(client_options=client_options)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        logger.info(f"✅ Connected to GCS bucket: {GCS_BUCKET_NAME}")
        return storage_client, bucket
    except Exception as e:
        logger.error(f"❌ Failed to connect to GCS: {e}")
        return None, None

def blob_exists(bucket, blob_name):
    """Check if a blob already exists in the bucket with timeout configuration."""
    if not bucket:
        return False
        
    blob = bucket.blob(blob_name)
    
    retry_config = google.api_core.retry.Retry(
        initial=1.0,
        maximum=30.0,
        multiplier=2,
        predicate=google.api_core.retry.if_exception_type(
            google.api_core.exceptions.DeadlineExceeded,
            google.api_core.exceptions.ServiceUnavailable,
            google.api_core.exceptions.TooManyRequests,
        ),
    )
    
    try:
        return blob.exists(timeout=60, retry=retry_config)
    except Exception as e:
        logger.warning(f"⚠️ Error checking if blob exists {blob_name}: {e}")
        return False

def upload_audio_to_gcs(bucket, audio_file, relative_path):
    """Upload an audio file to GCS with timeout and retry configuration."""
    if not bucket:
        return False
        
    try:
        blob_name = f"{GCS_PREFIX}/{relative_path}"
        
        if blob_exists(bucket, blob_name):
            logger.info(f"⏭️ Skipped (already exists): {audio_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
            return True
        
        blob = bucket.blob(blob_name)
        
        retry_config = google.api_core.retry.Retry(
            initial=1.0,
            maximum=60.0,
            multiplier=2,
            predicate=google.api_core.retry.if_exception_type(
                google.api_core.exceptions.DeadlineExceeded,
                google.api_core.exceptions.ServiceUnavailable,
                google.api_core.exceptions.TooManyRequests,
            ),
        )
        
        blob.upload_from_filename(
            audio_file,
            timeout=300,
            retry=retry_config
        )
        
        logger.info(f"☁️ Uploaded: {audio_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload {audio_file}: {e}")
        return False

def get_video_urls(channel_url):
    """Extract all video URLs from a YouTube channel playlist using cookies-from-browser."""
    logger.info(f"🔍 Fetching video list from: {channel_url}")
    start_time = datetime.now()
    
    ydl_opts = {
        'cookiesfrombrowser': ('firefox',),  # Use Firefox cookies
        'extract_flat': 'in_playlist',
        'quiet': True,
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(channel_url, download=False)
                if 'entries' in info_dict:
                    urls = [entry['url'] for entry in info_dict['entries'] if entry]
                    duration = datetime.now() - start_time
                    logger.info(f"✅ Found {len(urls)} videos from {channel_url} in {duration.total_seconds():.2f}s")
                    return urls
                else:
                    logger.warning(f"⚠️ No video entries found for {channel_url}")
                    return []
                    
        except Exception as e:
            error_message = str(e).lower()
            
            # Check for authentication errors
            if any(keyword in error_message for keyword in ['authentication', 'login', 'cookie', 'expired']):
                logger.error(f"❌ Authentication failed for {channel_url}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"🔄 Retrying authentication (attempt {attempt + 2}/{max_retries})...")
                    continue
                else:
                    logger.error("❌ Max authentication retries reached")
                    return []
            
            logger.error(f"❌ Could not extract video list for {channel_url} (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:
                return []
    
    return []

def download_and_upload_video_audio(video_url, download_path, bucket):
    """Downloads audio from a single video URL and uploads it to GCS."""
    logger.info(f"🎵 Starting download and upload: {video_url}")
    start_time = datetime.now()

    ydl_opts = {
        'cookiesfrombrowser': ('firefox',),
        'format': 'bestaudio[ext=wav]/bestaudio',
        'postprocessors': [],
        'postprocessor_args': [],
        'ignoreerrors': True,
        'outtmpl': os.path.join(download_path, '%(uploader)s/%(title)s.%(ext)s'),
        'prefer_ffmpeg': False,
        'quiet': True,
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                # First, extract info to get the expected filename
                info = ydl.extract_info(video_url, download=False)
                if not info:
                    logger.error(f"❌ Could not extract info for {video_url}")
                    return False, False
                
                # Construct the expected filename
                expected_filename = ydl.prepare_filename(info)
                expected_filename = os.path.splitext(expected_filename)[0] + '.wav'
                
                # Check if file already exists on GCS
                if bucket:
                    try:
                        relative_path = os.path.relpath(expected_filename, download_path)
                        blob_name = f"{GCS_PREFIX}/{relative_path}"
                        
                        if blob_exists(bucket, blob_name):
                            logger.info(f"⏭️ File already exists on GCS: {expected_filename} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
                            write_csv_entry(video_url, expected_filename, "ALREADY_EXISTS", 0, "File already on GCS")
                            return True, True  # Skip download, consider as success
                    except Exception as e:
                        logger.warning(f"⚠️ Error checking GCS existence: {e}")
                
                # Download the video
                ydl.download([video_url])
                
                # Check if the file was actually downloaded
                if os.path.exists(expected_filename):
                    duration = datetime.now() - start_time
                    logger.info(f"✅ Downloaded successfully: {video_url} (took {duration.total_seconds():.2f}s)")
                    write_csv_entry(video_url, expected_filename, "DOWNLOAD_SUCCESS", duration.total_seconds())
                    
                    # Upload to GCS
                    if bucket:
                        try:
                            # Get relative path from download directory
                            relative_path = os.path.relpath(expected_filename, download_path)
                            
                            if upload_audio_to_gcs(bucket, expected_filename, relative_path):
                                logger.info(f"☁️ Uploaded to GCS: {expected_filename}")
                                # Delete local file after successful upload
                                os.remove(expected_filename)
                                logger.info(f"🗑️ Deleted local file: {expected_filename}")
                                return True, True  # download success, upload success
                            else:
                                logger.warning(f"⚠️ Failed to upload to GCS: {expected_filename}")
                                return True, False  # download success, upload failed
                        except Exception as e:
                            logger.error(f"❌ Error during GCS upload: {e}")
                            return True, False  # download success, upload failed
                    else:
                        logger.warning(f"⚠️ No GCS bucket available, keeping local file: {expected_filename}")
                        return True, False  # download success, no upload
                else:
                    logger.error(f"❌ Downloaded file not found: {expected_filename}")
                    return False, False  # download failed
                    
        except Exception as e:
            error_message = str(e).lower()
            
            # Check for authentication errors
            if any(keyword in error_message for keyword in ['authentication', 'login', 'password', 'username', 'credentials']):
                logger.error(f"❌ Authentication failed for {video_url}: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"🔄 Retrying authentication (attempt {attempt + 2}/{max_retries})...")
                    continue
                else:
                    logger.error("❌ Max authentication retries reached")
                    duration = datetime.now() - start_time
                    write_csv_entry(video_url, "unknown", "AUTH_FAILED", duration.total_seconds(), str(e))
                    return False, False
            
            duration = datetime.now() - start_time
            logger.error(f"❌ Failed to download {video_url} after {duration.total_seconds():.2f}s (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:  # Last attempt
                write_csv_entry(video_url, "unknown", "DOWNLOAD_FAILED", duration.total_seconds(), str(e))
                return False, False  # download failed
    
    return False, False  # download failed

def download_channel_audio_parallel(channel_url, download_path, max_workers, bucket):
    """Downloads audio from all videos in a channel using parallel processing."""
    logger.info(f"🎬 Starting parallel download for channel: {channel_url}")
    
    # Get video URLs
    video_urls = get_video_urls(channel_url)
    
    if not video_urls:
        logger.warning(f"⚠️ No videos found for channel: {channel_url}")
        return 0, 0, 0
    
    logger.info(f"📊 Found {len(video_urls)} videos to download")
    
    # Initialize counters
    successful_downloads = 0
    successful_uploads = 0
    failed_downloads = 0
    
    # Use ThreadPoolExecutor for parallel downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_url = {
            executor.submit(download_and_upload_video_audio, url, download_path, bucket): url
            for url in video_urls
        }
        
        # Process completed downloads
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                download_success, upload_success = future.result()
                
                if download_success:
                    successful_downloads += 1
                    if upload_success:
                        successful_uploads += 1
                else:
                    failed_downloads += 1
                    
            except Exception as exc:
                logger.error(f"❌ {url} generated an exception: {exc}")
                failed_downloads += 1
    
    logger.info(f"🏁 Channel download completed: {channel_url}")
    logger.info(f"📊 Summary: {successful_downloads} successful downloads, {successful_uploads} successful uploads, {failed_downloads} failed")
    
    return successful_downloads, successful_uploads, failed_downloads

def main():
    """Main function to run the download process."""
    logger.info("🎬 Starting YouTube download script v2 (cookies-from-browser)")
    
    # Initialize GCS client
    storage_client, bucket = initialize_gcs_client()
    if not bucket:
        logger.warning("⚠️ GCS not available, will only download files locally")
    
    # Create download directory
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)
    
    # Process each channel
    total_downloads = 0
    total_uploads = 0
    total_failed = 0
    
    for channel_url in CHANNEL_URLS:
        logger.info(f"🎬 Processing channel: {channel_url}")
        
        downloads, uploads, failed = download_channel_audio_parallel(
            channel_url, DOWNLOAD_DIRECTORY, MAX_WORKERS, bucket
        )
        
        total_downloads += downloads
        total_uploads += uploads
        total_failed += failed
    
    # Final summary
    logger.info("🎉 Download process completed!")
    logger.info(f"📊 Final Summary:")
    logger.info(f"   Total downloads: {total_downloads}")
    logger.info(f"   Total uploads: {total_uploads}")
    logger.info(f"   Total failed: {total_failed}")

if __name__ == '__main__':
    main() 