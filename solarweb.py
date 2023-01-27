import time
import datetime
import json
import argparse
from collections import defaultdict
import sqlite3
import appdirs
import os

import requests
from urllib.parse import urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup

SOLARLOGGING_DATA_DIR = appdirs.user_data_dir("solarlogging", "mattsmith24")
SOLARLOGGING_DB_PATH = os.path.join(SOLARLOGGING_DATA_DIR, "solarlogging.db")
print(f"SOLARLOGGING_DB_PATH={SOLARLOGGING_DB_PATH}")


def is_daily_ts_newer_than_last_dailydata_timestamp(ts_datetime, last_dailydata_timestamp):
    return (
            last_dailydata_timestamp == None
            or ts_datetime > last_dailydata_timestamp
        )


def is_daily_ts_newer_than_yesterday(ts_datetime):
    yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    return (
        ts_datetime.day != yesterday.day
        and ts_datetime > yesterday
        )


def is_new_daily_ts(ts_datetime, last_dailydata_timestamp):
    return (
        is_daily_ts_newer_than_last_dailydata_timestamp(ts_datetime, last_dailydata_timestamp)
        and not is_daily_ts_newer_than_yesterday(ts_datetime)
    )

class SolarWeb:
    def __init__(self, debug=False) -> None:
        self.debug_enabled = debug
        self.config = None
        self.last_dailydata_timestamp = None
        self.requests_session = None
        self.pv_system_id = None
        self.sqlcon = None


    def debug(self, msg):
        if self.debug_enabled:
            print(msg)


    def init_dailydata(self):
        os.makedirs(SOLARLOGGING_DATA_DIR, exist_ok=True)
        self.sqlcon = sqlite3.connect(SOLARLOGGING_DB_PATH)
        self.sqlcon.row_factory = sqlite3.Row

        self.debug("init_dailydata: Initialising tables")
        with self.sqlcon:
            cur = self.sqlcon.execute("create table if not exists samples (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists daily (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists fiveminute (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists hourly (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists weekly (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists monthly (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")

        cur = self.sqlcon.cursor()
        for row in cur.execute("SELECT * from daily order by id desc limit 1"):
            self.last_dailydata_timestamp = datetime.datetime.fromisoformat(row["timestamp"])


    def login(self):
        print("Logging into solarweb")
        if self.requests_session != None:
            self.requests_session.close()
        self.requests_session = requests.Session()
        # Get a session
        try:
            external_login = self.requests_session.get("https://www.solarweb.com/Account/ExternalLogin")
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error accessing ExternalLogin: {e}")
            return False
        parsed_url = urlparse(external_login.url)
        query_dict = parse_qs(parsed_url.query)
        if external_login.status_code != 200 or not ("sessionDataKey" in query_dict):
            print("Error: Couldn't parse sessionDataKey from URL")
            print(external_login)
            print(external_login.url)
            print(external_login.text)
            return False
        session_data_key = query_dict['sessionDataKey'][0]
        # Login to fronius
        commonauth = self.requests_session.post("https://login.fronius.com/commonauth", data={
            "sessionDataKey": session_data_key,
            "username": self.config["username"],
            "password": self.config["password"],
            "chkRemember": "on"
        })
        if commonauth.status_code != 200:
            print("Error: posting to commonauth")
            print(commonauth)
            print(commonauth.url)
            print(commonauth.text)
            return False

        # Register login with Solarweb
        soup = BeautifulSoup(commonauth.text, 'html.parser')
        commonauth_form_data = {
            "code": soup.find("input", attrs={"name": "code"}).attrs["value"],
            "id_token": soup.find("input", attrs={"name": "id_token"}).attrs["value"],
            "state": soup.find("input", attrs={"name": "state"}).attrs["value"],
            "AuthenticatedIdPs": soup.find("input", attrs={"name": "AuthenticatedIdPs"}).attrs["value"],
            "session_state": soup.find("input", attrs={"name": "session_state"}).attrs["value"],
        }
        try:
        external_login_callback = self.requests_session.post("https://www.solarweb.com/Account/ExternalLoginCallback", data=commonauth_form_data)
        except requests.exceptions.ConnectionError as e:
            print(f"Exception when posting ExternalLoginCallback: {e}")
            return False

        # Get PV system ID
        parsed_url = urlparse(external_login_callback.url)
        query_dict = parse_qs(parsed_url.query)
        if external_login_callback.status_code != 200 or not ('pvSystemId' in query_dict):
            print("Error: Couldn't parse pvSystemId from URL")
            print(external_login_callback)
            print(external_login_callback.url)
            print(external_login_callback.text)
            return False
        self.pv_system_id = query_dict['pvSystemId'][0]
        print("Logged into solarweb. Begin polling data")
        return True


    def get_chart(self, chartday, interval, view):
        try:
            chart_data = self.requests_session.get(f"https://www.solarweb.com/Chart/GetChartNew?pvSystemId={self.pv_system_id}&year={chartday.year}&month={chartday.month}&day={chartday.day}&interval={interval}&view={view}")
            if chart_data.status_code != 200:
                print(chart_data)
                print(chart_data.url)
                print(chart_data.text)
                return None
            jsonchart = chart_data.json()
            if not jsonchart:
                print("get_chart: no json data returned")
                return None
            return jsonchart
        except requests.exceptions.ConnectionError as e:
            print(f"Exception reading chart for {chartday.year}-{chartday.month}-{chartday.day} {interval} {view}")
            print(f"{e}")
            return None


    def process_chart_data(self, yesterday):
        # Chart data is a json structure that wraps an array of timestamp / kwh values.
        # The timestamps can be parsed with datetime.datetime.fromtimestamp(val / 1000, tz=datetime.timezone.utc)
        chart_month_production = self.get_chart(yesterday, "month", "production")
        if chart_month_production == None:
            return False

        print(f"process_chart_data: last_dailydata_timestamp = {self.last_dailydata_timestamp}")
        found_new_data = False
        for data_tuple in chart_month_production["settings"]["series"][0]["data"]:
            print(f"process_chart_data: chart_month_production ts = {data_tuple[0]}")
            ts_datetime = datetime.datetime.fromtimestamp(int(data_tuple[0])/1000, tz=datetime.timezone.utc)
            if is_new_daily_ts(ts_datetime, self.last_dailydata_timestamp):
                found_new_data = True
                print("Timestamp is new")
                break
        if not found_new_data:
            print("process_chart_data: No new timestamps")
            return True

        # Get cumulative solar consumption data for the current month
        chart_month_consumption = self.get_chart(yesterday, "month", "consumption")
        if chart_month_consumption == None:
            return False

        # Extract the data series from the charts
        daily_data_tuples = {}
        for series in chart_month_production["settings"]["series"]:
            if series["name"] == "Energy to grid":
                daily_data_tuples["feedin"] = series["data"]
            if series["name"] == "Consumed directly":
                daily_data_tuples["direct"] = series["data"]
        for series in chart_month_consumption["settings"]["series"]:
            if series["name"] == "Energy from grid":
                daily_data_tuples["grid"] = series["data"]
        # Rearrange the series to group all series by timestamp
        daily_data_dict = {}
        for label in ["grid", "feedin", "direct"]:
            for tuple in daily_data_tuples[label]:
                ts = tuple[0]
                if ts not in daily_data_dict:
                    # Using defaultdict here will handle cases where these is a missing series for a timestamp
                    # and just return 0 in the next loop
                    daily_data_dict[ts] = defaultdict(int)
                daily_data_dict[ts][label] = tuple[1]
        # Ensure records are processed in order of timestamp, not by the whims of the dict key fn
        timestamps = list(daily_data_dict.keys())
        timestamps.sort(
            key = lambda ts: datetime.datetime.fromtimestamp(int(ts)/1000, tz=datetime.timezone.utc))
        last_insert_ts = None
        for ts in timestamps:
            data_dict = daily_data_dict[ts]
            ts_datetime = datetime.datetime.fromtimestamp(int(ts)/1000, tz=datetime.timezone.utc)
            self.debug(f"process_chart_data: Looking at data for ts {ts}")
            if is_new_daily_ts(ts_datetime, self.last_dailydata_timestamp):
                # solar generation = feedin + direct consumption
                # house user = direct consumption + grid
                entry = (ts_datetime.isoformat(), data_dict["grid"], data_dict["direct"] + data_dict["feedin"], data_dict["direct"] + data_dict["grid"])
                with self.sqlcon:
                    self.debug(f"process_chart_data: INSERT INTO daily (timestamp, grid, solar, home) VALUES (?, ?, ?, ?), {entry}")
                    self.sqlcon.execute("INSERT INTO daily (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)", entry)
                    last_insert_ts = ts_datetime
            else:
                if is_daily_ts_newer_than_last_dailydata_timestamp(ts_datetime, self.last_dailydata_timestamp):
                    self.debug("We already have this ts in the table")
                else:
                    self.debug("This ts is too new. We can't process daily data until the day is done")
        if last_insert_ts != None:
            self.last_dailydata_timestamp = last_insert_ts
            print(f"process_chart_data: New last_dailydata_timestamp = {self.last_dailydata_timestamp}")
        return True


    def process_aggregation(self, table, source_table, time_slot_fn, time_slot_increment_fn, convert_kwh_fn, deadline):
        cur = self.sqlcon.cursor()
        last_timestamp = None
        for row in cur.execute(f"SELECT * from {table} order by id desc limit 1"):
            last_timestamp = datetime.datetime.fromisoformat(row["timestamp"])
        if last_timestamp == None:
            # get first sample timestamp
            for row in cur.execute(f"SELECT * from {source_table} order by id asc limit 1"):
                last_timestamp = time_slot_fn(datetime.datetime.fromisoformat(row["timestamp"]))
        last_sample_timestamp = None
        for row in cur.execute(f"SELECT * from {source_table} order by id desc limit 1"):
            last_sample_timestamp = datetime.datetime.fromisoformat(row["timestamp"])
        if last_sample_timestamp != None:
            # Assume last_timestamp lines up with a slot start. The five minute slots are recorded
            # with the timestamp at the start of the slot.
            cur_timestamp = time_slot_increment_fn(last_timestamp)
            # Loop through as many slots as we can before time limit
            while cur_timestamp < time_slot_fn(last_sample_timestamp) \
                    and datetime.datetime.now(datetime.timezone.utc) < deadline - datetime.timedelta(seconds=5):
                cur_end_timestamp = time_slot_increment_fn(cur_timestamp)
                grid = 0
                solar = 0
                home = 0
                num_samples = 0
                for row in cur.execute(f"SELECT * from {source_table} WHERE timestamp >= ? and timestamp < ? order by id asc",
                        (cur_timestamp.isoformat(), cur_end_timestamp.isoformat())):
                    # Only accumulate +ve grid usage. Feedin can be calculated from 'home - solar'
                    if row['grid'] > 0:
                        grid += row['grid']
                    solar += row['solar']
                    home += row['home']
                    num_samples += 1
                if grid > 0.0 or solar > 0.0 or home > 0.0:
                    # Convert to kwh
                    if convert_kwh_fn != None:
                        grid = convert_kwh_fn(grid / num_samples)
                        solar = convert_kwh_fn(solar / num_samples)
                        home = convert_kwh_fn(home / num_samples)
                    with self.sqlcon:
                        self.debug(f"consolidate_data: INSERT INTO {table} (timestamp, grid, solar, home) VALUES ({cur_timestamp.isoformat()}, {grid}, {solar}, {home})")
                        self.sqlcon.execute(f"INSERT INTO {table} (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)",
                            (cur_timestamp.isoformat(), grid, solar, home))
                else:
                    # if there are large gaps in the data then sometimes this loop hits the deadline
                    # before it can find the end of the gap. In that case, this function will be run again
                    # from the last good timestamp, hit the gap, timeout and never get past that point.
                    # To avoid this, skip ahead to the next point in the source data that is greater
                    # than the cur_timestamp
                    for row in cur.execute(f"SELECT * from {source_table} where timestamp > ? \
                            and (grid != 0 or solar != 0 or home != 0) order by id asc limit 1",
                            (cur_timestamp.isoformat(),)):
                        cur_end_timestamp = time_slot_fn(datetime.datetime.fromisoformat(row["timestamp"]))
                        self.debug(f"process_aggregation {table}: Skip to cur_timestamp={cur_end_timestamp}")
                cur_timestamp = cur_end_timestamp


    def aggregate_data(self, deadline):
        def timestamp_5min_slot_in_hour(ts):
            """ Divide the hour into slots and return the start time of the slot
                that the current timestamp falls in. """
            res = ts.replace(minute=0, second=0, microsecond=0)
            slot_num = (ts - res) // datetime.timedelta(minutes=5)
            res += datetime.timedelta(minutes=5) * slot_num
            return res
        def add_five_minutes(ts):
            return ts + datetime.timedelta(minutes=5)
        def convert_fiveminute_to_kwh(val):
            return val / 1000.0 * 5.0 / 60.0
        self.process_aggregation("fiveminute", "samples", timestamp_5min_slot_in_hour, add_five_minutes, convert_fiveminute_to_kwh, deadline)

        def timestamp_hour(ts):
            return ts.replace(minute=0, second=0, microsecond=0)
        def add_hour(ts):
            return ts + datetime.timedelta(hours=1)
        def convert_hourly_to_kwh(val):
            return val / 1000.0
        self.process_aggregation("hourly", "samples", timestamp_hour, add_hour, convert_hourly_to_kwh, deadline)

        def timestamp_weekly(ts):
            # weekday() - Return the day of the week as an integer, where Monday is 0 and Sunday is 6.
            # Subtract ts.weekday() from the current date to get the start of the week.
            return ts.replace(hour=0, minute=0, second=0, microsecond=0) \
                - datetime.timedelta(days=ts.weekday())
        def add_week(ts):
            return ts + datetime.timedelta(days=7)
        self.process_aggregation("weekly", "daily", timestamp_weekly, add_week, None, deadline)

        def timestamp_monthly(ts):
            return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        def add_month(ts):
            return (ts + datetime.timedelta(days=31)).replace(day=1)
        self.process_aggregation("monthly", "daily", timestamp_monthly, add_month, None, deadline)


    def load_config(self):
        with open("solarweb.json") as fd:
            self.config = json.load(fd)


    def run(self):
        done = False
        self.load_config()
        self.init_dailydata()

        last_login_attempt = None
        while not done:
            # Delay logging in if we just made an attempt
            if last_login_attempt != None and (datetime.datetime.now() - last_login_attempt).seconds < 30:
                time.sleep(1)
                continue

            last_login_attempt = datetime.datetime.now()
            if not self.login():
                continue

            sampling_ok = False

            while True:
                # Get realtime solar data
                try:
                    actual_data_url = f"https://www.solarweb.com/ActualData/GetCompareDataForPvSystem?pvSystemId={self.pv_system_id}"
                    actual_data = self.requests_session.get(actual_data_url)
                except requests.exceptions.ConnectionError as e:
                    print(f"Exception while accessing: {actual_data_url}")
                    print(e.strerror)
                    print(e.winerror)
                    break
                if actual_data.status_code != 200:
                    print(actual_data)
                    print(actual_data.url)
                    print(actual_data.text)
                    break
                try:
                    pvdata_record = actual_data.json()
                except requests.exceptions.JSONDecodeError as e:
                    print(f"Exception while decoding pvdata")
                    print(e.strerror)
                    print(e.winerror)
                    print(actual_data)
                    print(actual_data.url)
                    print(actual_data.text)
                    break
                
                sample_time = datetime.datetime.now(datetime.timezone.utc)
                self.next_sample_time = sample_time + datetime.timedelta(seconds=30)
                pvdata_record["datetime"] = sample_time.isoformat()
                if "IsOnline" in pvdata_record and pvdata_record["IsOnline"] and "P_Grid" in pvdata_record \
                        and "P_PV" in pvdata_record and "P_Load" in pvdata_record:
                    if not sampling_ok:
                        sampling_ok = True
                        print(f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} Online")
                    grid = 0
                    if pvdata_record['P_Grid'] != None:
                        grid = pvdata_record['P_Grid']
                    pv = 0
                    if pvdata_record['P_PV'] != None:
                        pv = pvdata_record['P_PV']
                    home = 0
                    if pvdata_record['P_Load'] != None:
                        home = -pvdata_record['P_Load']

                    try:
                        with self.sqlcon:
                            self.debug(f"run: INSERT INTO samples (timestamp, grid, solar, home) VALUES ({pvdata_record['datetime']}, {grid}, {pv}, {home})")
                            self.sqlcon.execute("INSERT INTO samples (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)", 
                                (pvdata_record["datetime"], grid, pv, home))
                    except sqlite3.OperationalError as e:
                        print(f"Error saving data to sqlite DB: {e}")
                else:
                    print(f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} Offline: {json.dumps(pvdata_record)}")
                    sampling_ok = False

                # Get cumulative solar production data for yesterday, this is so that we get
                # full days totals across the month boundary
                yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
                yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
                if yesterday > self.last_dailydata_timestamp:
                    if not self.process_chart_data(yesterday):
                        break

                self.aggregate_data(self.next_sample_time)
                
                now = datetime.datetime.now(datetime.timezone.utc)
                if self.next_sample_time > now:
                    time.sleep((self.next_sample_time - now).total_seconds())

        self.sqlcon.close()
        if self.requests_session != None:
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
    process_date = datetime.datetime.strptime(solar_web.config["install_date"],"%Y-%m-%d")
    process_date.replace(tzinfo=datetime.timezone.utc)
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
        if process_date > datetime.datetime.now():
            break
    yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    solar_web.process_chart_data(yesterday)


def main():
    parser = argparse.ArgumentParser(description='Solar data logger')
    parser.add_argument('--history', action='store_true',
                        help='Process daily history since install date then exit. This will erase existing daily data (make a backup)')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug messages')

    args = parser.parse_args()
    if args.history:
        history()
        exit()

    solar_web = SolarWeb(debug=args.debug)
    solar_web.run()

 
if __name__=="__main__":
    main()