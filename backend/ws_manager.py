import asyncio
import json
import os
import logging
from typing import Dict, List
import yfinance as yf
from datetime import datetime
import pytz
from backend.data_fetcher import fetch_and_notify

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebSocketManager:
    _instance = None

    def __init__(self):
        self.running = False
        self.scheduler_task = None
        self.last_fetch_date = None  # Tracks the date of the last successful data fetch

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def start(self):
        """Starts the background task."""
        if self.running:
            return

        self.running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Scheduler Manager started (RVol disabled).")

    async def stop(self):
        """Stops the background task."""
        self.running = False
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        logger.info("Scheduler Manager stopped.")

    async def _scheduler_loop(self):
        """Independent loop for post-market scheduled tasks."""
        logger.info("Scheduler loop started.")
        while self.running:
            try:
                # Run every minute
                await asyncio.sleep(60)

                # --- Automatic Data Fetching Logic (16:15 ET) ---
                now_et = datetime.now(pytz.timezone('US/Eastern'))
                # Check if today is Monday-Friday (0-4)
                if now_et.weekday() <= 4:
                    # Target time: 16:15 ET
                    target_time = now_et.replace(hour=16, minute=15, second=0, microsecond=0)

                    # Trigger if:
                    # 1. Current time >= 16:15
                    # 2. We haven't successfully fetched for this date yet
                    if now_et >= target_time and self.last_fetch_date != now_et.date():
                        logger.info(f"Detected post-market time ({now_et}). Triggering automatic data fetch...")

                        # Determine which processes to run
                        # Short Term: Mon-Fri
                        run_short = True
                        # Long Term: Friday only
                        run_long = (now_et.weekday() == 4)

                        # Run fetch_and_notify in a separate thread because it's blocking/heavy
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, lambda: fetch_and_notify(run_short=run_short, run_long=run_long))

                        # Update last fetch date to prevent re-execution today
                        self.last_fetch_date = now_et.date()
                        logger.info(f"Automatic data fetch completed for {self.last_fetch_date}.")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60) # Wait before retry

    def get_all_rvols(self) -> Dict[str, float]:
        """Returns current RVol for all monitored tickers (Empty)."""
        return {}

# Global instance accessor
ws_manager = WebSocketManager.get_instance()
