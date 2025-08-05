#!/usr/bin/env python3
"""
Cookie Monitor Script
Periodically re-exports cookies from Firefox to keep them fresh.
This prevents cookie expiration issues during long-running download operations.
"""

import time
import logging
import os
import argparse
from datetime import datetime, timedelta
from download import export_firefox_cookies

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cookie_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CookieMonitor:
    def __init__(self, interval_minutes=30, max_age_hours=24):
        """
        Initialize the cookie monitor.
        
        Args:
            interval_minutes: How often to refresh cookies (default: 30 minutes)
            max_age_hours: Maximum age of cookies before forcing refresh (default: 24 hours)
        """
        self.interval_minutes = interval_minutes
        self.max_age_hours = max_age_hours
        self.cookies_file = 'cookies.txt'
        self.running = False
        
    def get_cookie_age(self):
        """Get the age of the cookies.txt file in hours."""
        if not os.path.exists(self.cookies_file):
            return float('inf')  # File doesn't exist, consider it very old
        
        file_time = os.path.getmtime(self.cookies_file)
        age_seconds = time.time() - file_time
        return age_seconds / 3600  # Convert to hours
    
    def should_refresh_cookies(self):
        """Determine if cookies should be refreshed based on age."""
        age_hours = self.get_cookie_age()
        
        if age_hours >= self.max_age_hours:
            logger.info(f"🔄 Cookies are {age_hours:.1f} hours old (max: {self.max_age_hours}h), forcing refresh")
            return True
        
        return False
    
    def refresh_cookies(self):
        """Export fresh cookies from Firefox."""
        try:
            logger.info("🔄 Refreshing cookies from Firefox...")
            start_time = datetime.now()
            
            export_firefox_cookies()
            
            duration = datetime.now() - start_time
            file_size = os.path.getsize(self.cookies_file) if os.path.exists(self.cookies_file) else 0
            
            logger.info(f"✅ Cookies refreshed successfully in {duration.total_seconds():.2f}s")
            logger.info(f"📄 Cookie file size: {file_size} bytes")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to refresh cookies: {e}")
            return False
    
    def run_once(self):
        """Run one iteration of cookie refresh check."""
        logger.info("🔍 Checking cookie status...")
        
        if self.should_refresh_cookies():
            return self.refresh_cookies()
        else:
            age_hours = self.get_cookie_age()
            logger.info(f"✅ Cookies are fresh ({age_hours:.1f}h old, max: {self.max_age_hours}h)")
            return True
    
    def run_continuous(self):
        """Run the cookie monitor continuously."""
        logger.info(f"🚀 Starting cookie monitor (refresh every {self.interval_minutes} minutes)")
        logger.info(f"⏰ Max cookie age: {self.max_age_hours} hours")
        
        self.running = True
        iteration = 0
        
        while self.running:
            try:
                iteration += 1
                logger.info(f"📊 Iteration {iteration}")
                
                success = self.run_once()
                
                if not success:
                    logger.warning("⚠️ Cookie refresh failed, will retry on next cycle")
                
                # Wait for next interval
                logger.info(f"⏳ Waiting {self.interval_minutes} minutes until next check...")
                time.sleep(self.interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("🛑 Cookie monitor stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in cookie monitor: {e}")
                logger.info("⏳ Waiting 5 minutes before retry...")
                time.sleep(300)  # Wait 5 minutes before retrying
        
        logger.info("🏁 Cookie monitor stopped")

def main():
    """Main function to run the cookie monitor."""
    parser = argparse.ArgumentParser(description='Monitor and refresh cookies from Firefox')
    parser.add_argument('--interval', type=int, default=30, 
                       help='Refresh interval in minutes (default: 30)')
    parser.add_argument('--max-age', type=int, default=24,
                       help='Maximum cookie age in hours (default: 24)')
    parser.add_argument('--once', action='store_true',
                       help='Run once and exit (don\'t run continuously)')
    parser.add_argument('--force', action='store_true',
                       help='Force refresh cookies immediately')
    
    args = parser.parse_args()
    
    monitor = CookieMonitor(
        interval_minutes=args.interval,
        max_age_hours=args.max_age
    )
    
    if args.force:
        logger.info("🔄 Force refreshing cookies...")
        success = monitor.refresh_cookies()
        if success:
            logger.info("✅ Force refresh completed successfully")
        else:
            logger.error("❌ Force refresh failed")
        return
    
    if args.once:
        logger.info("🔄 Running single cookie check...")
        success = monitor.run_once()
        if success:
            logger.info("✅ Single check completed successfully")
        else:
            logger.error("❌ Single check failed")
        return
    
    # Run continuously
    monitor.run_continuous()

if __name__ == "__main__":
    main() 