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
import browser_cookie3
from http.cookiejar import CookieJar

# --- Configuration ---
# A list of YouTube channel URLs to download from.
# It's best to go to the channel's "Videos" tab and use that URL.
def get_channel_urls():
    with open('channel_urls.txt', 'r') as f:
        return [line.strip() for line in f.readlines()]

CHANNEL_URLS = get_channel_urls()

# Define where to save the downloaded audio files.
# A subfolder with the channel's name will be created inside this directory.
DOWNLOAD_DIRECTORY = 'podcasts'

# Set the maximum number of concurrent downloads.
# Be mindful of your network bandwidth and CPU usage.
MAX_WORKERS = 5

# GCS Configuration
GCS_BUCKET_NAME = "multichannel-podcasts"
GCS_PREFIX = "raw_audio"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CSV tracking file
CSV_FILE = 'download_status.csv'
CSV_HEADERS = ['timestamp', 'url', 'filename', 'status', 'duration_seconds', 'error_message']

# Thread-safe counters
class DownloadCounters:
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

# ---------------------
def export_firefox_cookies(domain='youtube.com', output_path='cookies.txt'):
    # Load cookies from Firefox
    cj = browser_cookie3.firefox(domain_name=domain)
    # Save to Netscape format
    with open(output_path, 'w') as f:
        f.write('# Netscape HTTP Cookie File\n')
        for cookie in cj:
            if domain in cookie.domain:
                # Domain, include_subdomains, path, secure, expiry, name, value
                domain_field = cookie.domain
                include_subdomains = 'TRUE' if cookie.domain.startswith('.') else 'FALSE'
                path = cookie.path
                secure = 'TRUE' if cookie.secure else 'FALSE'
                expiry = str(int(cookie.expires)) if cookie.expires else '0'
                f.write(f"{domain_field}\t{include_subdomains}\t{path}\t{secure}\t{expiry}\t{cookie.name}\t{cookie.value}\n")
    return output_path

def write_csv_entry(url, filename, status, duration_seconds, error_message=""):
    """
    Writes a download status entry to the CSV file.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = [timestamp, url, filename, status, duration_seconds, error_message]
    
    # Create file with headers if it doesn't exist
    file_exists = os.path.exists(CSV_FILE)
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(CSV_HEADERS)
        writer.writerow(row)

def initialize_gcs_client():
    """
    Initialize Google Cloud Storage client with timeout configuration.
    """
    try:
        # Configure the client with custom timeout settings
        client_options = google.api_core.client_options.ClientOptions(
            api_endpoint="https://storage.googleapis.com",
            api_audience="https://storage.googleapis.com"
        )
        
        # Create client with timeout configuration
        storage_client = storage.Client(client_options=client_options)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        logger.info(f"✅ Connected to GCS bucket: {GCS_BUCKET_NAME}")
        return storage_client, bucket
    except Exception as e:
        logger.error(f"❌ Failed to connect to GCS: {e}")
        return None, None

def blob_exists(bucket, blob_name):
    """
    Check if a blob already exists in the bucket with timeout configuration.
    
    Args:
        bucket: GCS bucket object
        blob_name: Name of the blob to check
        
    Returns:
        bool: True if blob exists, False otherwise
    """
    if not bucket:
        return False
        
    blob = bucket.blob(blob_name)
    
    # Configure timeout for existence check
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
        return blob.exists(timeout=60, retry=retry_config)  # 1 minute timeout
    except Exception as e:
        logger.warning(f"⚠️ Error checking if blob exists {blob_name}: {e}")
        return False  # Assume it doesn't exist if we can't check

def upload_audio_to_gcs(bucket, audio_file, relative_path):
    """
    Upload an audio file to GCS with timeout and retry configuration.
    
    Args:
        bucket: GCS bucket object
        audio_file: Path to the audio file
        relative_path: Relative path for the blob name
        
    Returns:
        bool: True if upload successful, False otherwise
    """
    if not bucket:
        return False
        
    try:
        # Create GCS blob name with prefix
        blob_name = f"{GCS_PREFIX}/{relative_path}"
        
        # Check if blob already exists
        if blob_exists(bucket, blob_name):
            logger.info(f"⏭️ Skipped (already exists): {audio_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
            return True
        
        # Create blob and upload with timeout configuration
        blob = bucket.blob(blob_name)
        
        # Configure upload with longer timeout
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
        
        # Upload with retry configuration and longer timeout
        blob.upload_from_filename(
            audio_file,
            timeout=300,  # 5 minutes timeout
            retry=retry_config
        )
        
        logger.info(f"☁️ Uploaded: {audio_file} -> gs://{GCS_BUCKET_NAME}/{blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload {audio_file}: {e}")
        return False

def get_video_urls(channel_url):
    """
    Extracts all video URLs from a YouTube channel playlist.
    """
    logger.info(f"🔍 Fetching video list from: {channel_url}")
    start_time = datetime.now()
    
    # We use 'extract_flat' to quickly get video URLs without full metadata
    ydl_opts = {
        'cookiefile': 'cookies.txt',
        'extract_flat': 'in_playlist',
        'quiet': True,
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(channel_url, download=False)
                if 'entries' in info_dict:
                    # We need the full URL for yt-dlp to process in the next step
                    urls = [entry['url'] for entry in info_dict['entries'] if entry]
                    duration = datetime.now() - start_time
                    logger.info(f"✅ Found {len(urls)} videos from {channel_url} in {duration.total_seconds():.2f}s")
                    return urls
                else:
                    logger.warning(f"⚠️  No video entries found for {channel_url}")
                    return []
                    
        except Exception as e:
            error_message = str(e).lower()
            
            # Check if it's a cookie expiration error
            if any(keyword in error_message for keyword in ['cookie', 'expired', 'authentication', 'login']):
                logger.error(f"❌ Cookie expiration detected on attempt {attempt + 1} for {channel_url}")
                
                if attempt < max_retries - 1:  # Don't refresh cookies on the last attempt
                    try:
                        logger.info(f"🔄 Refreshing cookies from Firefox...")
                        export_firefox_cookies()
                        logger.info(f"✅ Cookies refreshed, retrying video list fetch...")
                        continue  # Retry with fresh cookies
                    except Exception as cookie_error:
                        logger.error(f"❌ Failed to refresh cookies: {cookie_error}")
                        # Continue with existing cookies
            else:
                logger.error(f"❌ Could not extract video list for {channel_url} (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:  # Last attempt
                return []
    
    return []

def download_and_upload_video_audio(video_url, download_path, bucket):
    """
    Downloads audio from a single video URL and uploads it to GCS.
    This function is executed by each thread.
    """
    logger.info(f"🎵 Starting download and upload: {video_url}")
    start_time = datetime.now()

    ydl_opts = {
        'cookiefile': 'cookies.txt',
        'format': 'bestaudio[ext=m4a]/bestaudio',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        'ignoreerrors': True,
        'outtmpl': os.path.join(download_path, '%(uploader)s/%(title)s.%(ext)s'),
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
            
            # Check if it's a cookie expiration error
            if any(keyword in error_message for keyword in ['cookie', 'expired', 'authentication', 'login']):
                logger.error(f"❌ Cookie expiration detected on attempt {attempt + 1} for {video_url}")
                
                if attempt < max_retries - 1:  # Don't refresh cookies on the last attempt
                    try:
                        logger.info(f"🔄 Refreshing cookies from Firefox...")
                        export_firefox_cookies()
                        logger.info(f"✅ Cookies refreshed, retrying download...")
                        continue  # Retry with fresh cookies
                    except Exception as cookie_error:
                        logger.error(f"❌ Failed to refresh cookies: {cookie_error}")
                        # Continue with existing cookies
                
            duration = datetime.now() - start_time
            logger.error(f"❌ Failed to download {video_url} after {duration.total_seconds():.2f}s (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:  # Last attempt
                write_csv_entry(video_url, "unknown", "DOWNLOAD_FAILED", duration.total_seconds(), str(e))
                return False, False  # download failed
    
    return False, False  # download failed

def download_channel_audio_parallel(channel_url, download_path, max_workers, bucket=None):
    """
    Downloads all audio from a YouTube channel using multiple threads and uploads to GCS.
    """
    logger.info(f"🚀 Starting channel download: {channel_url}")
    channel_start_time = datetime.now()
    
    video_urls = get_video_urls(channel_url)
    if not video_urls:
        logger.error(f"No videos found for {channel_url} or failed to fetch video list.")
        return

    logger.info(f"📊 Found {len(video_urls)} videos for {channel_url}. Starting download with {max_workers} workers.")

    # Ensure the main download directory exists
    if not os.path.exists(download_path):
        os.makedirs(download_path)
        logger.info(f"📁 Created download directory: {download_path}")

    successful_downloads = 0
    successful_uploads = 0
    failed_downloads = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks to the thread pool and collect results
        future_to_url = {executor.submit(download_and_upload_video_audio, url, download_path, bucket): url for url in video_urls}
        
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
            except Exception as e:
                logger.error(f"❌ Unexpected error for {url}: {e}")
                failed_downloads += 1

    channel_duration = datetime.now() - channel_start_time
    logger.info(f"🏁 Channel download completed for {channel_url}")
    logger.info(f"📈 Summary: {successful_downloads} successful downloads, {successful_uploads} successful uploads, {failed_downloads} failed")
    logger.info(f"⏱️  Total time: {channel_duration.total_seconds():.2f}s")

if __name__ == '__main__':
    logger.info("🎬 Starting YouTube channel audio downloader with GCS upload")
    total_start_time = datetime.now()
    
    # Initialize GCS client
    storage_client, bucket = initialize_gcs_client()
    if not bucket:
        logger.warning("⚠️ GCS not available, will only download files locally")
    
    for url in CHANNEL_URLS:
        if 'youtube.com' not in url:
            logger.error(f"'{url}' is not a valid YouTube URL. Please check the CHANNEL_URLS list.")
        else:
            download_channel_audio_parallel(url, DOWNLOAD_DIRECTORY, MAX_WORKERS, bucket)
    
    total_duration = datetime.now() - total_start_time
    logger.info(f"🎉 All channel downloads completed in {total_duration.total_seconds():.2f}s")
