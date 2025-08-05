#!/usr/bin/env python3
"""
Channel Splitter Script
Reads a list of channels and splits them into N groups based on number of videos to download.
This helps distribute work evenly across multiple download processes.
"""

import yt_dlp
import os
import json
import logging
from datetime import datetime
import argparse
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('split.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_channel_urls():
    """Read channel URLs from channel_urls.txt file."""
    try:
        with open('channel_urls.txt', 'r') as f:
            return [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        logger.error("❌ channel_urls.txt file not found")
        return []

def count_videos_in_channel(channel_url):
    """Count the number of videos in a YouTube channel."""
    logger.info(f"🔍 Counting videos in: {channel_url}")
    start_time = datetime.now()
    
    ydl_opts = {
        'cookiefile': 'cookies.txt',  # Use cookies.txt file
        'extract_flat': 'in_playlist',
        'quiet': True,
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(channel_url, download=False)
                if 'entries' in info_dict:
                    video_count = len([entry for entry in info_dict['entries'] if entry])
                    duration = datetime.now() - start_time
                    logger.info(f"✅ Found {video_count} videos in {channel_url} (took {duration.total_seconds():.2f}s)")
                    return video_count
                else:
                    logger.warning(f"⚠️ No video entries found for {channel_url}")
                    return 0
                    
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
                    return 0
            
            logger.error(f"❌ Could not count videos for {channel_url} (attempt {attempt + 1}): {e}")
            
            if attempt == max_retries - 1:
                return 0
    
    return 0

def analyze_channels(channel_urls):
    """Analyze all channels and count their videos."""
    logger.info(f"📊 Analyzing {len(channel_urls)} channels...")
    
    channel_data = {}
    total_videos = 0
    
    for i, channel_url in enumerate(channel_urls, 1):
        logger.info(f"📊 Processing channel {i}/{len(channel_urls)}: {channel_url}")
        
        video_count = count_videos_in_channel(channel_url)
        
        channel_data[channel_url] = {
            'video_count': video_count,
            'channel_name': channel_url.split('/')[-1] if '/' in channel_url else channel_url
        }
        
        total_videos += video_count
    
    logger.info(f"✅ Analysis complete! Total videos across all channels: {total_videos}")
    return channel_data, total_videos

def split_channels_balanced(channel_data, num_groups):
    """Split channels into N groups with balanced video distribution."""
    logger.info(f"🎯 Splitting {len(channel_data)} channels into {num_groups} groups...")
    
    # Sort channels by video count (descending)
    sorted_channels = sorted(channel_data.items(), key=lambda x: x[1]['video_count'], reverse=True)
    
    # Initialize groups
    groups = [[] for _ in range(num_groups)]
    group_video_counts = [0] * num_groups
    
    # Distribute channels using greedy approach
    for channel_url, data in sorted_channels:
        # Find group with minimum video count
        min_group_idx = group_video_counts.index(min(group_video_counts))
        
        # Add channel to that group
        groups[min_group_idx].append({
            'url': channel_url,
            'video_count': data['video_count'],
            'channel_name': data['channel_name']
        })
        
        # Update group video count
        group_video_counts[min_group_idx] += data['video_count']
    
    return groups, group_video_counts

def save_groups_to_files(groups, group_video_counts, output_dir="split_groups"):
    """Save each group to a separate file."""
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"💾 Saving groups to {output_dir}/ directory...")
    
    for i, (group, video_count) in enumerate(zip(groups, group_video_counts), 1):
        filename = os.path.join(output_dir, f"group_{i}.txt")
        
        with open(filename, 'w') as f:
            for channel in group:
                f.write(f"{channel['url']}\n")
        
        logger.info(f"📄 Group {i}: {len(group)} channels, {video_count} videos -> {filename}")
    
    # Save summary
    summary_file = os.path.join(output_dir, "summary.json")
    summary = {
        'total_groups': len(groups),
        'groups': []
    }
    
    for i, (group, video_count) in enumerate(zip(groups, group_video_counts), 1):
        group_summary = {
            'group_number': i,
            'channel_count': len(group),
            'video_count': video_count,
            'channels': group
        }
        summary['groups'].append(group_summary)
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"📊 Summary saved to: {summary_file}")

def print_summary(groups, group_video_counts):
    """Print a summary of the split results."""
    logger.info("📊 SPLIT SUMMARY")
    logger.info("=" * 50)
    
    total_channels = sum(len(group) for group in groups)
    total_videos = sum(group_video_counts)
    
    logger.info(f"📈 Total channels: {total_channels}")
    logger.info(f"📈 Total videos: {total_videos}")
    logger.info(f"📈 Number of groups: {len(groups)}")
    logger.info(f"📈 Average videos per group: {total_videos / len(groups):.1f}")
    
    logger.info("\n📋 Group Details:")
    for i, (group, video_count) in enumerate(zip(groups, group_video_counts), 1):
        logger.info(f"   Group {i}: {len(group)} channels, {video_count} videos")
        for channel in group:
            logger.info(f"     - {channel['channel_name']}: {channel['video_count']} videos")
        logger.info("")

def main():
    """Main function to run the channel splitting process."""
    parser = argparse.ArgumentParser(description='Split YouTube channels into balanced groups')
    parser.add_argument('--groups', type=int, default=4, help='Number of groups to split into (default: 4)')
    parser.add_argument('--output-dir', default='split_groups', help='Output directory for group files (default: split_groups)')
    parser.add_argument('--skip-analysis', action='store_true', help='Skip video counting and use existing analysis')
    parser.add_argument('--analysis-file', default='channel_analysis.json', help='File to save/load channel analysis')
    
    args = parser.parse_args()
    
    logger.info("🎬 Starting channel splitter")
    logger.info(f"📊 Target groups: {args.groups}")
    logger.info(f"📁 Output directory: {args.output_dir}")
    
    # Get channel URLs
    channel_urls = get_channel_urls()
    if not channel_urls:
        logger.error("❌ No channels found to process")
        return
    
    logger.info(f"📋 Found {len(channel_urls)} channels to process")
    
    # Analyze channels (or load existing analysis)
    if args.skip_analysis and os.path.exists(args.analysis_file):
        logger.info(f"📂 Loading existing analysis from {args.analysis_file}")
        with open(args.analysis_file, 'r') as f:
            channel_data = json.load(f)
        total_videos = sum(data['video_count'] for data in channel_data.values())
    else:
        logger.info("🔍 Analyzing channels...")
        channel_data, total_videos = analyze_channels(channel_urls)
        
        # Save analysis
        logger.info(f"💾 Saving analysis to {args.analysis_file}")
        with open(args.analysis_file, 'w') as f:
            json.dump(channel_data, f, indent=2)
    
    # Split channels into groups
    groups, group_video_counts = split_channels_balanced(channel_data, args.groups)
    
    # Print summary
    print_summary(groups, group_video_counts)
    
    # Save groups to files
    save_groups_to_files(groups, group_video_counts, args.output_dir)
    
    logger.info("🎉 Channel splitting completed!")

if __name__ == "__main__":
    main()
