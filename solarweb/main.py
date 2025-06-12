"""Main entry point and command-line interface for solar data logging."""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import appdirs

from .client import SolarWebClient
from .processor import SolarDataProcessor, SOLARLOGGING_DB_PATH


def history():
    """Process daily history since install date then exit."""
    solar_web = SolarWeb()
    solar_web.load_config()
    solar_web.processor.init_database()

    # Delete daily data so we can re-populate it
    with solar_web.processor.sqlcon:
        solar_web.processor.sqlcon.execute("DELETE FROM daily")
    solar_web.processor.last_dailydata_timestamp = None

    if not solar_web.client.login():
        return

    # Parse install date and ensure it's timezone-aware
    process_date = datetime.strptime(solar_web.config["install_date"], "%Y-%m-%d")
    process_date = process_date.replace(tzinfo=timezone.utc)
    
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        print(process_date.isoformat())
        chart_month_production = solar_web.client.get_chart(process_date, "month", "production")
        chart_month_consumption = solar_web.client.get_chart(process_date, "month", "consumption")
        if not solar_web.processor.process_chart_data(chart_month_production, chart_month_consumption):
            break

        # Calculate next month
        new_year = process_date.year
        new_month = process_date.month + 1
        if new_month > 12:
            new_month = 1
            new_year += 1
        process_date = process_date.replace(month=new_month, year=new_year)
        
        if process_date > yesterday:
            break

    # Process yesterday's data
    chart_month_production = solar_web.client.get_chart(yesterday, "month", "production")
    chart_month_consumption = solar_web.client.get_chart(yesterday, "month", "consumption")
    solar_web.processor.process_chart_data(chart_month_production, chart_month_consumption)


def delete_small_aggregated_data(database):
    """Delete small aggregated data from the database."""
    if not database:
        database = SOLARLOGGING_DB_PATH
    sqlcon = sqlite3.connect(database)
    with sqlcon:
        sqlcon.execute("DELETE FROM fiveminute")
        sqlcon.execute("DELETE FROM hourly")
    sqlcon.close()
    print("Small aggregated data deleted.")


class SolarWeb:
    """Main class for managing solar data collection and processing.
    
    This class coordinates between the SolarWebClient and SolarDataProcessor
    to collect and process solar data.
    """
    
    def __init__(self, debug=False, database="") -> None:
        """Initialize SolarWeb manager.
        
        Args:
            debug: Enable debug logging
            database: Optional path to SQLite database file
        """
        self.debug_enabled = debug
        self.config = None
        self.client = None
        self.processor = SolarDataProcessor(database, debug)

    def load_config(self):
        """Load configuration from solarweb.json."""
        with open("solarweb.json") as fd:
            self.config = json.load(fd)
        self.client = SolarWebClient(self.config, self.debug_enabled)

    def run(self):
        """Main loop for polling and processing solar data."""
        try:
            self.load_config()
            self.processor.init_database()

            while True:
                if not self.client.throttled_login():
                    continue

                while True:
                    # Get and process real-time data
                    pvdata_record = self.client.get_realtime_data()
                    if pvdata_record is None:
                        break
                    self.processor.process_realtime_data(pvdata_record)

                    # Get and process daily data
                    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
                    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                    
                    if yesterday > self.processor.last_dailydata_timestamp:
                        chart_month_production = self.client.get_chart(yesterday, "month", "production")
                        chart_month_consumption = self.client.get_chart(yesterday, "month", "consumption")
                        if chart_month_production is None or chart_month_consumption is None:
                            break
                        self.processor.process_chart_data(chart_month_production, chart_month_consumption)

                    # Aggregate data
                    self.processor.aggregate_data(self.processor.next_sample_time)

                    # Wait until next sample time
                    now = datetime.now(timezone.utc)
                    if self.processor.next_sample_time > now:
                        time.sleep((self.processor.next_sample_time - now).total_seconds())

        except Exception as e:
            print(f"Error in main loop: {e}")
            if self.debug_enabled:
                import traceback
                traceback.print_exc()
        finally:
            # Cleanup resources
            self.processor.close()
            if self.client is not None:
                self.client.close()


def main():
    """Main entry point."""
    # Make stdout line-buffered (i.e. each line will be automatically flushed):
    sys.stdout.reconfigure(line_buffering=True)
    
    parser = argparse.ArgumentParser(description='Solar data logger')
    parser.add_argument('--history', action='store_true',
                        help='Process daily history since install date then exit. This will erase existing daily data (make a backup)')
    parser.add_argument('--delete-small-aggregated-data', action='store_true',
                        help='Delete aggregated data from the database for fiveminute and hourly tables. This will not delete daily, weekly or monthly data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug messages')
    parser.add_argument('--database', help='Path to sqlite3 database')

    args = parser.parse_args()
    if args.history:
        history()
        exit()

    if args.delete_small_aggregated_data:
        delete_small_aggregated_data(args.database)
        exit()

    solar_web = SolarWeb(debug=args.debug, database=args.database)
    solar_web.run()


if __name__=="__main__":
    main() 