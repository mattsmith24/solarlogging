"""Process and store solar data in the database."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import appdirs

from .models import RealtimeData
from .utils import is_new_timestamp, timestamp_newer_than
from .aggregator import (
    FiveMinuteAggregator,
    HourlyAggregator,
    WeeklyAggregator,
    MonthlyAggregator
)

SOLARLOGGING_DATA_DIR = appdirs.user_data_dir("solarlogging", "mattsmith24")
SOLARLOGGING_DB_PATH = Path(SOLARLOGGING_DATA_DIR, "solarlogging.db")


class SolarDataProcessor:
    """Processes and stores solar data in the database.
    
    This class handles:
    - Database initialization and management
    - Processing and storing real-time data
    - Processing and storing historical data
    - Aggregating data into different time intervals
    """
    
    def __init__(self, database="", debug=False):
        """Initialize data processor.
        
        Args:
            database: Optional path to SQLite database file
            debug: Enable debug logging
        """
        self.debug_enabled = debug
        self.database = SOLARLOGGING_DB_PATH if not database else Path(database)
        print(f"database={self.database.resolve()}")
        
        self.sqlcon = None
        self.last_dailydata_timestamp = None
        self.next_sample_time = datetime.now(timezone.utc)  # Initialize with current time
        self.sampling_ok = False

    def debug(self, msg):
        """Log debug message if debug mode is enabled."""
        if self.debug_enabled:
            print(msg)

    def init_database(self):
        """Initialize database and tables for storing solar data."""
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.sqlcon = sqlite3.connect(self.database)
        self.sqlcon.row_factory = sqlite3.Row

        self.debug("init_database: Initialising tables")
        with self.sqlcon:
            cur = self.sqlcon.cursor()
            # Create tables if they don't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS daily (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fiveminute (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weekly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS monthly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    grid real,
                    solar real,
                    home real,
                    timestamp text
                )
            """)

        # Get last daily data timestamp
        cur = self.sqlcon.cursor()
        for row in cur.execute("SELECT * from daily order by id desc limit 1"):
            self.last_dailydata_timestamp = datetime.fromisoformat(row["timestamp"])

    def process_realtime_data(self, data):
        """Process and store real-time solar data.
        
        Args:
            data: RealtimeData object containing the data to process
            
        Returns:
            bool: True if data was processed successfully
        """
        self.next_sample_time = datetime.now(timezone.utc) + timedelta(seconds=30)
 
        if not data.is_online:
            self.debug("process_realtime_data: PV system is offline")
            return False
            
        # Store the sample
        with self.sqlcon:
            self.sqlcon.execute(
                "INSERT INTO samples (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)",
                (data.timestamp.isoformat(), data.grid, data.solar, data.home)
            )
            
        self.debug(f"process_realtime_data: Stored sample: {data.timestamp.isoformat()}, {data.grid:.2f}, {data.solar:.2f}, {data.home:.2f}")
        return True

    def process_chart_data(self, chart_month_production, chart_month_consumption):
        """Process chart data and store in database.
        
        Args:
            chart_month_production: Production data from SolarWeb
            chart_month_consumption: Consumption data from SolarWeb
        """

        self.debug(f"process_chart_data: last_dailydata_timestamp = {self.last_dailydata_timestamp}")
        
        # Check for new data
        found_new_data = False
        for data_tuple in chart_month_production["settings"]["series"][0]["data"]:
            self.debug(f"process_chart_data: chart_month_production ts = {data_tuple[0]}")
            ts_datetime = datetime.fromtimestamp(int(data_tuple[0])/1000, tz=timezone.utc)
            if is_new_timestamp(ts_datetime, self.last_dailydata_timestamp):
                found_new_data = True
                self.debug("Timestamp is new")
                break
                
        if not found_new_data:
            self.debug("process_chart_data: No new timestamps")
            return

        # Extract data series
        daily_data_tuples = {}
        for series in chart_month_production["settings"]["series"]:
            if series["name"] == "Energy to grid":
                daily_data_tuples["feedin"] = series["data"]
            if series["name"] == "Consumed directly":
                daily_data_tuples["direct"] = series["data"]
                
        for series in chart_month_consumption["settings"]["series"]:
            if series["name"] == "Energy from grid":
                daily_data_tuples["grid"] = series["data"]

        # Group data by timestamp
        daily_data_dict = {}
        for label in ["grid", "feedin", "direct"]:
            for tuple in daily_data_tuples[label]:
                ts = tuple[0]
                if ts not in daily_data_dict:
                    daily_data_dict[ts] = {}
                daily_data_dict[ts][label] = tuple[1]

        # Process data in timestamp order
        timestamps = sorted(
            daily_data_dict.keys(),
            key=lambda ts: datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc)
        )
        
        last_insert_ts = None
        for ts in timestamps:
            data_dict = daily_data_dict[ts]
            ts_datetime = datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc)
            self.debug(f"process_chart_data: Looking at data for ts {ts}")
            
            if is_new_timestamp(ts_datetime, self.last_dailydata_timestamp):
                # Calculate values:
                # solar = feedin + direct consumption
                # home = direct consumption + grid
                entry = (
                    ts_datetime.isoformat(),
                    data_dict["grid"],
                    data_dict["direct"] + data_dict["feedin"],
                    data_dict["direct"] + data_dict["grid"]
                )
                
                with self.sqlcon:
                    self.debug(f"process_chart_data: INSERT INTO daily (timestamp, grid, solar, home) VALUES (?, ?, ?, ?), {entry}")
                    self.sqlcon.execute(
                        "INSERT INTO daily (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)",
                        entry
                    )
                    last_insert_ts = ts_datetime
            else:
                if timestamp_newer_than(ts_datetime, self.last_dailydata_timestamp):
                    self.debug("This ts is too new. We can't process daily data until the day is done")
                else:
                    self.debug("We already have this ts in the table")
                    
        if last_insert_ts is not None:
            self.last_dailydata_timestamp = last_insert_ts
            self.debug(f"process_chart_data: New last_dailydata_timestamp = {self.last_dailydata_timestamp}")
            
    def aggregate_data(self, deadline):
        """Aggregate data into different time intervals.
        
        Args:
            deadline: Processing deadline
        """
        FiveMinuteAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        HourlyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        WeeklyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        MonthlyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)

    def close(self):
        """Close database connection."""
        if self.sqlcon is not None:
            self.sqlcon.close() 