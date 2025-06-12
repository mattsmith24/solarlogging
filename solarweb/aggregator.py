"""Data aggregation classes for different time intervals."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from collections import defaultdict


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
            last_timestamp = datetime.fromisoformat(source_data[-1]["timestamp"])
            if last_timestamp >= self.time_slot_increment(slot_start_timestamp):
                self.debug(f"aggregate_data: {self.table}: Found at least one slot worth of data")
                break
                
            if last_timestamp >= last_source_timestamp:
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