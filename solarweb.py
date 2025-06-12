import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from abc import ABC, abstractmethod

import appdirs
import requests
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, urlparse

SOLARLOGGING_DATA_DIR = appdirs.user_data_dir("solarlogging", "mattsmith24")
SOLARLOGGING_DB_PATH = Path(SOLARLOGGING_DATA_DIR, "solarlogging.db")

# Make stdout line-buffered (i.e. each line will be automatically flushed):
sys.stdout.reconfigure(line_buffering=True)

def timestamp_newer_than(timestamp, other_timestamp):
    """Check if a timestamp is newer than the other timestamp.
       If the other timestamp is None, then assume the timestamp is new."""
    return (
        other_timestamp is None
        or timestamp > other_timestamp
    )


def timestamp_newer_than_or_equal_to_today(timestamp):
    """Check if a timestamp is newer than or equal to today."""
    today = datetime.now(timezone.utc)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    return timestamp >= today


def is_new_timestamp(ts_datetime, last_dailydata_timestamp):
    """Check if the timestamp is newer than the last daily data timestamp
       and not newer than yesterday. This is to avoid processing data that
       is not yet complete (e.g. the current day)."""
    return (
        timestamp_newer_than(ts_datetime, last_dailydata_timestamp)
        and not timestamp_newer_than_or_equal_to_today(ts_datetime)
    )


class DataAggregator(ABC):
    """Abstract base class for aggregating solar data at different time intervals.
    
    This class provides the framework for aggregating solar data (grid, solar, home)
    into different time slots (5-minute, hourly, weekly, monthly). Each subclass
    implements specific time slot logic and unit conversion.
    """
    
    def __init__(self, sqlcon, debug=False):
        """Initialize the aggregator.
        
        Args:
            sqlcon: SQLite database connection
            debug: Enable debug logging
        """
        self.sqlcon = sqlcon
        self.debug_enabled = debug
        self.table = None
        self.source_table = None

    def debug(self, msg):
        """Log debug message if debug mode is enabled."""
        if self.debug_enabled:
            print(msg)

    @abstractmethod
    def time_slot(self, dt):
        """Get the start time of the time slot that contains the given timestamp.
        
        Args:
            dt: datetime object to find slot for
            
        Returns:
            datetime: Start time of the containing slot
        """
        pass

    @abstractmethod
    def time_slot_increment(self, dt, increment=1):
        """Get the start time of the next time slot.
        
        Args:
            dt: Current slot start time
            increment: Number of slots to increment by
            
        Returns:
            datetime: Start time of the next slot
        """
        pass

    @abstractmethod
    def convert_units(self, value, num_samples):
        """Convert raw values to appropriate units for this aggregation level.
        
        Args:
            value: Raw value to convert
            num_samples: Number of samples used to calculate the value
            
        Returns:
            float: Converted value
        """
        pass

    def deadline_expired(self, deadline):
        """Check if the processing deadline has expired.
        
        Args:
            deadline: datetime object representing the deadline
            
        Returns:
            bool: True if deadline has expired (with 5 second grace period)
        """
        return datetime.now(timezone.utc) >= deadline - timedelta(seconds=5)

    def get_last_aggregate_timestamp(self):
        """Get the last timestamp from the aggregation table.
        
        If no data exists, returns the start time of the first source data.
        
        Returns:
            datetime: Last timestamp in aggregation table or first source data timestamp
        """
        cur = self.sqlcon.cursor()
        last_timestamp = None
        
        # Get last timestamp from aggregation table
        for row in cur.execute(f"SELECT * from {self.table} order by id desc limit 1"):
            last_timestamp = datetime.fromisoformat(row["timestamp"])
            
        # If no aggregation data, get first source data timestamp
        if last_timestamp is None:
            for row in cur.execute(f"SELECT * from {self.source_table} order by id asc limit 1"):
                last_timestamp = self.time_slot(datetime.fromisoformat(row["timestamp"]))
                
        return last_timestamp

    def get_source_data(self, deadline, cur, slot_start_timestamp, last_source_timestamp):
        """Get source data for aggregation.
        
        Args:
            deadline: Processing deadline
            cur: Database cursor
            slot_start_timestamp: Start time of first slot to process
            last_source_timestamp: Last available source data timestamp
            
        Returns:
            list: Source data rows
        """
        source_data = []
        
        while not self.deadline_expired(deadline):
            # Query about 2000 slots worth of data at a time
            query_end_timestamp = self.time_slot_increment(slot_start_timestamp, 2000)
            
            # Get data for current time range
            for row in cur.execute(
                f"SELECT * from {self.source_table} WHERE timestamp >= ? and timestamp < ? order by id asc",
                (slot_start_timestamp.isoformat(), query_end_timestamp.isoformat())
            ):
                source_data.append(row)
                
            self.debug(f"aggregate_data: {self.table}: Found {len(source_data)} rows from {slot_start_timestamp} to {query_end_timestamp}")
            # We must get at least one slot worth of data or reach the end of the source data
            if datetime.fromisoformat(source_data[-1]["timestamp"]) >= self.time_slot_increment(slot_start_timestamp):
                self.debug(f"aggregate_data: {self.table}: Found at least one slot worth of data")
                break
                
            if datetime.fromisoformat(source_data[-1]["timestamp"]) >= last_source_timestamp:
                self.debug(f"aggregate_data: {self.table}: Reached end of data. Not enough data to aggregate")
                break
                
            # Look for next slot with data
            found_more_data = False
            for row in cur.execute(
                f"SELECT * from {self.source_table} where timestamp > ? and (grid != 0 or solar != 0 or home != 0) order by id asc limit 1",
                (self.time_slot_increment(slot_start_timestamp).isoformat(),)
            ):
                slot_start_timestamp = self.time_slot(datetime.fromisoformat(row["timestamp"]))
                found_more_data = True
                self.debug(f"aggregate_data: {self.table}: Found more data after gap at cur_timestamp={slot_start_timestamp}")
                
            if not found_more_data:
                self.debug(f"aggregate_data: {self.table}: No more data found after {slot_start_timestamp}")
                break
                
        return source_data

    def process_aggregation(self, deadline):
        """Process data aggregation up to the deadline.
        
        Args:
            deadline: Processing deadline
        """
        cur = self.sqlcon.cursor()
        
        # Get last processed timestamp and last available source data
        last_aggregate_timestamp = self.get_last_aggregate_timestamp()
        last_source_timestamp = None
        
        for row in cur.execute(f"SELECT * from {self.source_table} order by id desc limit 1"):
            last_source_timestamp = datetime.fromisoformat(row["timestamp"])
            
        if last_source_timestamp is None:
            self.debug(f"process_aggregation: {self.table}: No source data found")
            return
            
        # Start processing from next slot after last processed
        slot_start_timestamp = self.time_slot_increment(last_aggregate_timestamp)
        aggregate_rows = []
        
        # Get source data for processing
        source_data = self.get_source_data(deadline, cur, slot_start_timestamp, last_source_timestamp)
        
        # Process slots until deadline or end of data
        while (source_data and 
               slot_start_timestamp < self.time_slot(datetime.fromisoformat(source_data[-1]["timestamp"])) and 
               not self.deadline_expired(deadline)):
            
            slot_end_timestamp = self.time_slot_increment(slot_start_timestamp)
            
            # Initialize slot totals
            grid = 0
            solar = 0
            home = 0
            num_samples = 0
            
            # Get rows for current slot
            slot_rows = [
                row for row in source_data
                if datetime.fromisoformat(row["timestamp"]) >= slot_start_timestamp
                and datetime.fromisoformat(row["timestamp"]) < slot_end_timestamp
            ]
            
            # Sum values for slot
            for row in slot_rows:
                if row['grid'] > 0:  # Only accumulate positive grid usage
                    grid += row['grid']
                solar += row['solar']
                home += row['home']
                num_samples += 1
                
            # Convert and store if we have data
            if grid > 0.0 or solar > 0.0 or home > 0.0:
                grid = self.convert_units(grid, num_samples)
                solar = self.convert_units(solar, num_samples)
                home = self.convert_units(home, num_samples)
                
                aggregate_rows.append({
                    "timestamp": slot_start_timestamp.isoformat(),
                    "grid": grid,
                    "solar": solar,
                    "home": home
                })
                
                self.debug(f"aggregate_data: {self.table}: ({slot_start_timestamp.isoformat()}, {grid:.2f}, {solar:.2f}, {home:.2f})")
            else:
                # Skip to next data point if no data in current slot
                slot_rows = [
                    row for row in source_data
                    if datetime.fromisoformat(row["timestamp"]) > slot_start_timestamp
                    and (row["grid"] != 0 or row["solar"] != 0 or row["home"] != 0)
                ]
                
                if slot_rows:
                    row = slot_rows[0]
                    slot_end_timestamp = self.time_slot(datetime.fromisoformat(row["timestamp"]))
                    self.debug(f"process_aggregation {self.table}: Skip to slot_start_timestamp={slot_end_timestamp}")
                    
            slot_start_timestamp = slot_end_timestamp
            
        # Save aggregated data
        if aggregate_rows:
            with self.sqlcon:
                self.debug(f"process_aggregation: Inserting {len(aggregate_rows)} rows into {self.table}")
                self.sqlcon.executemany(
                    f"INSERT INTO {self.table} (timestamp, grid, solar, home) VALUES (:timestamp, :grid, :solar, :home)",
                    aggregate_rows
                )
                
        self.debug(f"process_aggregation: {self.table}: done")


class FiveMinuteAggregator(DataAggregator):
    """Aggregates data into 5-minute intervals."""
    
    def __init__(self, sqlcon, debug=False):
        super().__init__(sqlcon, debug)
        self.table = "fiveminute"
        self.source_table = "samples"

    def time_slot(self, dt):
        """Get start of 5-minute slot containing the timestamp."""
        res = dt.replace(minute=0, second=0, microsecond=0)
        slot_num = (dt - res) // timedelta(minutes=5)
        return res + timedelta(minutes=5) * slot_num
    
    def time_slot_increment(self, dt, increment=1):
        """Get start of next 5-minute slot."""
        return dt + (timedelta(minutes=5) * increment)

    def convert_units(self, value, num_samples):
        """Convert to average kW."""
        return value / num_samples if num_samples > 0 else 0.0


class HourlyAggregator(DataAggregator):
    """Aggregates data into hourly intervals."""
    
    def __init__(self, sqlcon, debug=False):
        super().__init__(sqlcon, debug)
        self.table = "hourly"
        self.source_table = "samples"

    def time_slot(self, dt):
        """Get start of hour containing the timestamp."""
        return dt.replace(minute=0, second=0, microsecond=0)

    def time_slot_increment(self, dt, increment=1):
        """Get start of next hour."""
        return dt + (timedelta(hours=1) * increment)

    def convert_units(self, value, num_samples):
        """Convert to average kW."""
        return value / num_samples if num_samples > 0 else 0.0


class WeeklyAggregator(DataAggregator):
    """Aggregates data into weekly intervals starting on Monday."""
    
    def __init__(self, sqlcon, debug=False):
        super().__init__(sqlcon, debug)
        self.table = "weekly"
        self.source_table = "daily"

    def time_slot(self, dt):
        """Get start of week (Monday) containing the timestamp."""
        return dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=dt.weekday())

    def time_slot_increment(self, dt, increment=1):
        """Get start of next week."""
        return dt + (timedelta(days=7) * increment)

    def convert_units(self, value, _num_samples):
        """Keep values in kWh."""
        return value


class MonthlyAggregator(DataAggregator):
    """Aggregates data into monthly intervals."""
    
    def __init__(self, sqlcon, debug=False):
        super().__init__(sqlcon, debug)
        self.table = "monthly"
        self.source_table = "daily"

    def time_slot(self, dt):
        """Get start of month containing the timestamp."""
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    def time_slot_increment(self, dt, increment=1):
        """Get start of next month."""
        for _ in range(increment):
            dt = dt + timedelta(days=31)
            dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return dt

    def convert_units(self, value, _num_samples):
        """Keep values in kWh."""
        return value

class SolarWeb:
    """Main class for interacting with SolarWeb API and managing solar data.
    
    This class handles:
    - Authentication with SolarWeb
    - Fetching real-time and historical solar data
    - Storing data in SQLite database
    - Aggregating data into different time intervals
    """
    
    def __init__(self, debug=False, database="") -> None:
        """Initialize SolarWeb client.
        
        Args:
            debug: Enable debug logging
            database: Optional path to SQLite database file
        """
        self.debug_enabled = debug
        self.database = SOLARLOGGING_DB_PATH if not database else Path(database)
        print(f"database={self.database.resolve()}")
        
        # Initialize instance variables
        self.config = None
        self.last_dailydata_timestamp = None
        self.requests_session = None
        self.pv_system_id = None
        self.sqlcon = None
        self.last_login_attempt = None
        self.sampling_ok = False
        self.next_sample_time = None

    def debug(self, msg):
        """Log debug message if debug mode is enabled."""
        if self.debug_enabled:
            print(msg)

    def init_dailydata(self):
        """Initialize database and tables for storing solar data."""
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.sqlcon = sqlite3.connect(self.database)
        self.sqlcon.row_factory = sqlite3.Row

        self.debug("init_dailydata: Initialising tables")
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

    def login(self):
        """Authenticate with SolarWeb and get PV system ID.
        
        Returns:
            bool: True if login successful, False otherwise
        """
        print("Logging into solarweb")
        
        # Close existing session if any
        if self.requests_session is not None:
            self.requests_session.close()
        self.requests_session = requests.Session()

        try:
            # Get initial session
            external_login = self.requests_session.get("https://www.solarweb.com/Account/ExternalLogin")
            if external_login.status_code != 200:
                print("Error: Failed to access ExternalLogin")
                self.debug(external_login)
                self.debug(external_login.url)
                self.debug(external_login.text)
                return False

            # Parse session data key
            parsed_url = urlparse(external_login.url)
            query_dict = parse_qs(parsed_url.query)
            if "sessionDataKey" not in query_dict:
                print("Error: Couldn't parse sessionDataKey from URL")
                self.debug(external_login)
                self.debug(external_login.url)
                self.debug(external_login.text)
                return False
            session_data_key = query_dict['sessionDataKey'][0]

            # Login to Fronius
            commonauth = self.requests_session.post(
                "https://login.fronius.com/commonauth",
                data={
                    "sessionDataKey": session_data_key,
                    "username": self.config["username"],
                    "password": self.config["password"],
                    "chkRemember": "on"
                }
            )
            if commonauth.status_code != 200:
                print("Error: Failed to post to commonauth")
                self.debug(commonauth)
                self.debug(commonauth.url)
                self.debug(commonauth.text)
                return False

            # Parse login response
            soup = BeautifulSoup(commonauth.text, 'html.parser')
            try:
                commonauth_form_data = {
                    "code": soup.find("input", attrs={"name": "code"}).attrs["value"],
                    "id_token": soup.find("input", attrs={"name": "id_token"}).attrs["value"],
                    "state": soup.find("input", attrs={"name": "state"}).attrs["value"],
                    "AuthenticatedIdPs": soup.find("input", attrs={"name": "AuthenticatedIdPs"}).attrs["value"],
                    "session_state": soup.find("input", attrs={"name": "session_state"}).attrs["value"],
                }
            except AttributeError as e:
                print(f"Exception when parsing commonauth form data: {e}")
                return False

            # Complete login process
            external_login_callback = self.requests_session.post(
                "https://www.solarweb.com/Account/ExternalLoginCallback",
                data=commonauth_form_data
            )
            if external_login_callback.status_code != 200:
                print("Error: Failed to complete login process")
                self.debug(external_login_callback)
                self.debug(external_login_callback.url)
                self.debug(external_login_callback.text)
                return False

            # Get PV system ID
            parsed_url = urlparse(external_login_callback.url)
            query_dict = parse_qs(parsed_url.query)
            if 'pvSystemId' not in query_dict:
                print("Error: Couldn't parse pvSystemId from URL")
                self.debug(external_login_callback)
                self.debug(external_login_callback.url)
                self.debug(external_login_callback.text)
                return False

            self.pv_system_id = query_dict['pvSystemId'][0]
            print("Logged into solarweb. Begin polling data")
            return True

        except requests.exceptions.ConnectionError as e:
            print(f"Connection error during login: {e}")
            return False

    def get_chart(self, chartday, interval, view):
        """Get chart data from SolarWeb.
        
        Args:
            chartday: Date to get data for
            interval: Data interval (e.g. 'month')
            view: Data view type (e.g. 'production', 'consumption')
            
        Returns:
            dict: Chart data or None if request failed
        """
        try:
            chart_data = self.requests_session.get(
                f"https://www.solarweb.com/Chart/GetChartNew",
                params={
                    "pvSystemId": self.pv_system_id,
                    "year": chartday.year,
                    "month": chartday.month,
                    "day": chartday.day,
                    "interval": interval,
                    "view": view
                }
            )
            
            if chart_data.status_code != 200:
                self.debug(chart_data)
                self.debug(chart_data.url)
                self.debug(chart_data.text)
                return None
                
            jsonchart = chart_data.json()
            if not jsonchart:
                self.debug("get_chart: no json data returned")
                return None
                
            return jsonchart
            
        except requests.exceptions.ConnectionError as e:
            self.debug(f"Exception reading chart for {chartday.year}-{chartday.month}-{chartday.day} {interval} {view}")
            self.debug(f"{e}")
            return None

    def process_chart_data(self, yesterday):
        """Process chart data for a given date.
        
        Args:
            yesterday: Date to process data for
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        # Get production data
        chart_month_production = self.get_chart(yesterday, "month", "production")
        if chart_month_production is None:
            return False

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
            return True

        # Get consumption data
        chart_month_consumption = self.get_chart(yesterday, "month", "consumption")
        if chart_month_consumption is None:
            return False

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
                    daily_data_dict[ts] = defaultdict(int)
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
            
        return True

    def aggregate_data(self, deadline):
        """Aggregate data into different time intervals.
        
        Args:
            deadline: Processing deadline
        """
        FiveMinuteAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        HourlyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        WeeklyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)
        MonthlyAggregator(self.sqlcon, self.debug_enabled).process_aggregation(deadline)

    def load_config(self):
        """Load configuration from solarweb.json."""
        with open("solarweb.json") as fd:
            self.config = json.load(fd)

    def get_realtime_data(self):
        """Get real-time solar data from SolarWeb.
        
        Returns:
            dict: Real-time data or None if request failed
        """
        try:
            actual_data = self.requests_session.get(
                "https://www.solarweb.com/ActualData/GetCompareDataForPvSystem",
                params={"pvSystemId": self.pv_system_id}
            )
            
            if actual_data.status_code != 200:
                self.debug(actual_data)
                self.debug(actual_data.url)
                self.debug(actual_data.text)
                return None
                
            return actual_data.json()
            
        except (requests.exceptions.ConnectionError, requests.exceptions.JSONDecodeError) as e:
            self.debug(f"Exception while getting realtime data: {e}")
            return None

    def throttled_login(self):
        """Attempt login with rate limiting.
        
        Returns:
            bool: True if login successful, False otherwise
        """
        # Delay login if we just made an attempt
        if self.last_login_attempt is not None and (datetime.now() - self.last_login_attempt).seconds < 30:
            time.sleep(1)
            return False

        self.last_login_attempt = datetime.now()
        self.sampling_ok = False
        return self.login()

    def poll_realtime_data(self):
        """Poll and store real-time solar data.
        
        Returns:
            bool: True if polling successful, False otherwise
        """
        pvdata_record = self.get_realtime_data()
        if pvdata_record is None:
            return False
        
        sample_time = datetime.now(timezone.utc)
        self.next_sample_time = sample_time + timedelta(seconds=30)
        pvdata_record["datetime"] = sample_time.isoformat()
        
        if ("IsOnline" in pvdata_record and pvdata_record["IsOnline"] and 
            "P_Grid" in pvdata_record and "P_PV" in pvdata_record and "P_Load" in pvdata_record):
            
            if not self.sampling_ok:
                self.sampling_ok = True
                print("Online")
                
            # Extract values with defaults
            grid = pvdata_record.get('P_Grid', 0) or 0
            pv = pvdata_record.get('P_PV', 0) or 0
            home = -(pvdata_record.get('P_Load', 0) or 0)

            try:
                with self.sqlcon:
                    self.debug(f"run: INSERT INTO samples (timestamp, grid, solar, home) VALUES ({pvdata_record['datetime']}, {grid:.2f}, {pv:.2f}, {home:.2f})")
                    self.sqlcon.execute(
                        "INSERT INTO samples (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)",
                        (pvdata_record["datetime"], grid, pv, home)
                    )
            except sqlite3.OperationalError as e:
                self.debug(f"Error saving data to sqlite DB: {e}")
        else:
            print(f"Offline: {json.dumps(pvdata_record)}")
            self.sampling_ok = False

        return True

    def poll_daily_data(self):
        """Poll and store daily solar data.
        
        Returns:
            bool: True if polling successful, False otherwise
        """
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if yesterday > self.last_dailydata_timestamp:
            if not self.process_chart_data(yesterday):
                return False
                
        return True

    def run(self):
        """Main loop for polling and processing solar data."""
        try:
            self.load_config()
            self.init_dailydata()

            while True:
                if not self.throttled_login():
                    continue

                while True:
                    if not self.poll_realtime_data():
                        break
                    if not self.poll_daily_data():
                        break
                        
                    self.aggregate_data(self.next_sample_time)

                    # Wait until next sample time
                    now = datetime.now(timezone.utc)
                    if self.next_sample_time > now:
                        time.sleep((self.next_sample_time - now).total_seconds())

        except Exception as e:
            print(f"Error in main loop: {e}")
            if self.debug_enabled:
                import traceback
                traceback.print_exc()
        finally:
            # Cleanup resources
            if self.sqlcon is not None:
                self.sqlcon.close()
            if self.requests_session is not None:
                self.requests_session.close()

def history():
    solar_web = SolarWeb()
    solar_web.load_config()
    solar_web.init_dailydata()

    # Delete daily data so we can re-populate it
    with solar_web.sqlcon:
        solar_web.sqlcon.execute("DELETE FROM daily")
    solar_web.last_dailydata_timestamp = None

    if not solar_web.login():
        return
    process_date = datetime.strptime(solar_web.config["install_date"],"%Y-%m-%d")
    process_date.replace(tzinfo=timezone.utc)
    while True:
        print(process_date.isoformat())
        if not solar_web.process_chart_data(process_date):
            break
        new_year = process_date.year
        new_month = process_date.month + 1
        if new_month > 12:
            new_month = 1
            new_year += 1
        process_date = process_date.replace(month=new_month, year=new_year)
        if process_date > datetime.now():
            break
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    solar_web.process_chart_data(yesterday)

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

def main():
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
