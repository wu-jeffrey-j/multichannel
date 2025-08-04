#!/usr/bin/env python3
"""
Script to sample MP3 and M4A files from podcasts directory and extract metadata.
Samples 100 audio files (1 per subfolder) and reports sample rate and bit rate.
"""

import os
import glob
import random
import csv
from datetime import datetime
import logging
from pathlib import Path
import mutagen
from mutagen.mp3 import MP3
from mutagen.mp4 import MP4

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# CSV output file
CSV_FILE = 'audio_metadata_sample.csv'
CSV_HEADERS = ['timestamp', 'file_path', 'folder_name', 'file_size_bytes', 'duration_seconds', 
               'sample_rate_hz', 'bit_rate_kbps', 'channels', 'format']

def write_csv_entry(file_path, folder_name, file_size, duration, sample_rate, bit_rate, channels, format_info):
    """
    Writes metadata entry to CSV file.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    row = [timestamp, file_path, folder_name, file_size, duration, sample_rate, bit_rate, channels, format_info]
    
    # Create file with headers if it doesn't exist
    file_exists = os.path.exists(CSV_FILE)
    
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(CSV_HEADERS)
        writer.writerow(row)

def get_audio_metadata(file_path):
    """
    Extract metadata from an audio file (MP3 or M4A).
    
    Args:
        file_path: Path to the audio file
        
    Returns:
        dict: Metadata including sample rate, bit rate, duration, etc.
    """
    try:
        # Get file size
        file_size = os.path.getsize(file_path)
        
        # Determine file format and load appropriate metadata
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.mp3':
            # Load MP3 file with mutagen
            audio = MP3(file_path)
            format_info = 'MP3'
            
            # Extract MP3 metadata
            metadata = {
                'file_size_bytes': file_size,
                'duration_seconds': audio.info.length if hasattr(audio.info, 'length') else None,
                'sample_rate_hz': audio.info.sample_rate if hasattr(audio.info, 'sample_rate') else None,
                'bit_rate_kbps': audio.info.bitrate // 1000 if hasattr(audio.info, 'bitrate') else None,
                'channels': audio.info.channels if hasattr(audio.info, 'channels') else None,
                'format': format_info
            }
            
        elif file_ext == '.m4a':
            # Load M4A file with mutagen
            audio = MP4(file_path)
            format_info = 'M4A'
            
            # Extract M4A metadata
            metadata = {
                'file_size_bytes': file_size,
                'duration_seconds': audio.info.length if hasattr(audio.info, 'length') else None,
                'sample_rate_hz': audio.info.sample_rate if hasattr(audio.info, 'sample_rate') else None,
                'bit_rate_kbps': audio.info.bitrate // 1000 if hasattr(audio.info, 'bitrate') else None,
                'channels': audio.info.channels if hasattr(audio.info, 'channels') else None,
                'format': format_info
            }
            
        else:
            logger.warning(f"⚠️ Unsupported file format: {file_ext}")
            return None
        
        return metadata
        
    except Exception as e:
        logger.error(f"❌ Failed to extract metadata from {file_path}: {e}")
        return None

def find_audio_folders(base_dir="podcasts"):
    """
    Find all folders containing MP3 or M4A files.
    
    Args:
        base_dir: Base directory to search
        
    Returns:
        list: List of folder paths that contain audio files
    """
    if not os.path.exists(base_dir):
        logger.error(f"❌ Directory {base_dir} does not exist")
        return []
    
    # Find all directories that contain audio files
    audio_folders = []
    
    for root, dirs, files in os.walk(base_dir):
        # Check if this directory contains MP3 or M4A files
        audio_files = [f for f in files if f.lower().endswith(('.mp3', '.m4a'))]
        if audio_files:
            audio_folders.append(root)
    
    logger.info(f"📁 Found {len(audio_folders)} folders containing audio files")
    return audio_folders

def sample_audio_files(folders, sample_size=100):
    """
    Sample audio files from folders (1 per folder).
    
    Args:
        folders: List of folder paths
        sample_size: Maximum number of files to sample
        
    Returns:
        list: List of (folder_path, audio_file_path) tuples
    """
    samples = []
    
    # Shuffle folders to get random sampling
    random.shuffle(folders)
    
    for folder in folders[:sample_size]:
        # Find audio files in this folder
        audio_files = glob.glob(os.path.join(folder, "*.mp3")) + glob.glob(os.path.join(folder, "*.m4a"))
        
        if audio_files:
            # Randomly select one audio file from this folder
            selected_file = random.choice(audio_files)
            samples.append((folder, selected_file))
    
    logger.info(f"📊 Sampled {len(samples)} audio files from {len(samples)} folders")
    return samples

def analyze_metadata(samples):
    """
    Analyze metadata from sampled audio files.
    
    Args:
        samples: List of (folder_path, audio_file_path) tuples
        
    Returns:
        dict: Summary statistics
    """
    metadata_list = []
    successful_count = 0
    failed_count = 0
    format_counts = {'MP3': 0, 'M4A': 0}
    
    for folder_path, audio_file in samples:
        logger.info(f"🔍 Analyzing: {os.path.basename(audio_file)}")
        
        metadata = get_audio_metadata(audio_file)
        
        if metadata:
            # Add folder and file info
            folder_name = os.path.basename(folder_path)
            metadata['file_path'] = audio_file
            metadata['folder_name'] = folder_name
            
            metadata_list.append(metadata)
            successful_count += 1
            format_counts[metadata['format']] += 1
            
            # Write to CSV
            write_csv_entry(
                audio_file, folder_name, metadata['file_size_bytes'],
                metadata['duration_seconds'], metadata['sample_rate_hz'],
                metadata['bit_rate_kbps'], metadata['channels'], metadata['format']
            )
        else:
            failed_count += 1
    
    # Calculate summary statistics
    if metadata_list:
        sample_rates = [m['sample_rate_hz'] for m in metadata_list if m['sample_rate_hz']]
        bit_rates = [m['bit_rate_kbps'] for m in metadata_list if m['bit_rate_kbps']]
        durations = [m['duration_seconds'] for m in metadata_list if m['duration_seconds']]
        file_sizes = [m['file_size_bytes'] for m in metadata_list if m['file_size_bytes']]
        
        summary = {
            'total_files': len(metadata_list),
            'successful_count': successful_count,
            'failed_count': failed_count,
            'format_counts': format_counts,
            'sample_rate_stats': {
                'min': min(sample_rates) if sample_rates else None,
                'max': max(sample_rates) if sample_rates else None,
                'unique_values': list(set(sample_rates)) if sample_rates else []
            },
            'bit_rate_stats': {
                'min': min(bit_rates) if bit_rates else None,
                'max': max(bit_rates) if bit_rates else None,
                'unique_values': list(set(bit_rates)) if bit_rates else []
            },
            'duration_stats': {
                'min': min(durations) if durations else None,
                'max': max(durations) if durations else None,
                'avg': sum(durations) / len(durations) if durations else None
            },
            'file_size_stats': {
                'min': min(file_sizes) if file_sizes else None,
                'max': max(file_sizes) if file_sizes else None,
                'avg': sum(file_sizes) / len(file_sizes) if file_sizes else None
            }
        }
    else:
        summary = {
            'total_files': 0,
            'successful_count': 0,
            'failed_count': failed_count,
            'format_counts': format_counts,
            'sample_rate_stats': {},
            'bit_rate_stats': {},
            'duration_stats': {},
            'file_size_stats': {}
        }
    
    return summary

def print_summary(summary):
    """
    Print summary statistics.
    
    Args:
        summary: Summary statistics dictionary
    """
    logger.info("📊 AUDIO METADATA ANALYSIS SUMMARY")
    logger.info("=" * 50)
    
    logger.info(f"📁 Total files analyzed: {summary['total_files']}")
    logger.info(f"✅ Successful: {summary['successful_count']}")
    logger.info(f"❌ Failed: {summary['failed_count']}")
    
    # Show format breakdown
    if summary['format_counts']:
        logger.info("📊 Format breakdown:")
        for format_type, count in summary['format_counts'].items():
            if count > 0:
                logger.info(f"   {format_type}: {count} files")
    
    if summary['sample_rate_stats']['unique_values']:
        logger.info(f"🎵 Sample Rates: {summary['sample_rate_stats']['unique_values']} Hz")
        logger.info(f"   Range: {summary['sample_rate_stats']['min']} - {summary['sample_rate_stats']['max']} Hz")
    
    if summary['bit_rate_stats']['unique_values']:
        logger.info(f"🔊 Bit Rates: {summary['bit_rate_stats']['unique_values']} kbps")
        logger.info(f"   Range: {summary['bit_rate_stats']['min']} - {summary['bit_rate_stats']['max']} kbps")
    
    if summary['duration_stats']['avg']:
        logger.info(f"⏱️ Duration: {summary['duration_stats']['min']:.1f}s - {summary['duration_stats']['max']:.1f}s")
        logger.info(f"   Average: {summary['duration_stats']['avg']:.1f}s")
    
    if summary['file_size_stats']['avg']:
        avg_size_mb = summary['file_size_stats']['avg'] / (1024 * 1024)
        logger.info(f"💾 File Size: {summary['file_size_stats']['min'] / (1024*1024):.1f}MB - {summary['file_size_stats']['max'] / (1024*1024):.1f}MB")
        logger.info(f"   Average: {avg_size_mb:.1f}MB")

def main():
    """
    Main function to run the metadata analysis.
    """
    logger.info("🎬 Starting audio metadata analysis")
    start_time = datetime.now()
    
    # Find folders containing audio files
    audio_folders = find_audio_folders()
    
    if not audio_folders:
        logger.error("❌ No audio folders found")
        return
    
    # Sample audio files
    samples = sample_audio_files(audio_folders, sample_size=100)
    
    if not samples:
        logger.error("❌ No audio files found to sample")
        return
    
    # Analyze metadata
    summary = analyze_metadata(samples)
    
    # Print summary
    print_summary(summary)
    
    # Log completion
    duration = datetime.now() - start_time
    logger.info(f"✅ Analysis complete! (took {duration.total_seconds():.2f}s)")
    logger.info(f"📄 Results saved to: {CSV_FILE}")

if __name__ == "__main__":
    main()
