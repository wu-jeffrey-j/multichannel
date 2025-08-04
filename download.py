import yt_dlp
import os
import concurrent.futures
import logging
from datetime import datetime
import csv

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
# ---------------------

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
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
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
            logger.error(f"❌ Could not extract video list for {channel_url}: {e}")
            return []


def download_video_audio(video_url, download_path):
    """
    Downloads audio from a single video URL.
    This function is executed by each thread.
    """
    logger.info(f"🎵 Starting download: {video_url}")
    start_time = datetime.now()
    
    # ydl_opts = {
    #     'format': 'bestaudio/best',
    #     'cookiefile': 'cookies.txt',
    #     'postprocessors': [{
    #         'key': 'FFmpegExtractAudio',
    #         'preferredcodec': 'mp3',
    #         'preferredquality': '192',
    #     }],
    #     # Save files to a directory named after the channel inside the download_path
    #     'outtmpl': 
    #     'ignoreerrors': True,
    #     'quiet': True,  # Keep the console output clean during parallel downloads
    # }

    ydl_opts = {
        'cookiefile': 'cookies.txt',
        # Select the best audio-only format (M4A or original codec) and skip ffmpeg postprocessing
        'format': 'bestaudio[ext=m4a]/bestaudio',
        # Do not run any postprocessors (no ffmpeg conversion)
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
        # Merge-output-format ensures container if needed
        # 'merge_output_format': 'm4a',
        'ignoreerrors': True,
        # Output template (title).m4a
        'outtmpl': os.path.join(download_path, '%(uploader)s/%(title)s.%(ext)s'),
        'quiet': True,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
            duration = datetime.now() - start_time
            filename = ydl.prepare_filename(ydl.extract_info(video_url, download=False))
            logger.info(f"✅ Downloaded successfully: {video_url} (took {duration.total_seconds():.2f}s)")
            write_csv_entry(video_url, filename, "DOWNLOAD_SUCCESS", duration.total_seconds())
            return True
    except Exception as e:
        duration = datetime.now() - start_time
        logger.error(f"❌ Failed to download {video_url} after {duration.total_seconds():.2f}s: {e}")
        write_csv_entry(video_url, filename, "DOWNLOAD_FAILED", duration.total_seconds(), str(e))
        return False


def download_channel_audio_parallel(channel_url, download_path, max_workers):
    """
    Downloads all audio from a YouTube channel using multiple threads.
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
    failed_downloads = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks to the thread pool and collect results
        future_to_url = {executor.submit(download_video_audio, url, download_path): url for url in video_urls}
        
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                if result:
                    successful_downloads += 1
                else:
                    failed_downloads += 1
            except Exception as e:
                logger.error(f"❌ Unexpected error for {url}: {e}")
                failed_downloads += 1

    channel_duration = datetime.now() - channel_start_time
    logger.info(f"🏁 Channel download completed for {channel_url}")
    logger.info(f"📈 Summary: {successful_downloads} successful, {failed_downloads} failed downloads")
    logger.info(f"⏱️  Total time: {channel_duration.total_seconds():.2f}s")


if __name__ == '__main__':
    logger.info("🎬 Starting YouTube channel audio downloader")
    total_start_time = datetime.now()
    
    for url in CHANNEL_URLS:
        if 'youtube.com' not in url:
            logger.error(f"'{url}' is not a valid YouTube URL. Please check the CHANNEL_URLS list.")
        else:
            download_channel_audio_parallel(url, DOWNLOAD_DIRECTORY, MAX_WORKERS)
    
    total_duration = datetime.now() - total_start_time
    logger.info(f"🎉 All channel downloads completed in {total_duration.total_seconds():.2f}s")
