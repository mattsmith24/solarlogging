import time
import datetime
import json
import argparse
from collections import defaultdict
import sqlite3

import requests
from urllib.parse import urlparse
from urllib.parse import parse_qs
from bs4 import BeautifulSoup

def is_new_ts(ts_datetime, last_dailydata_timestamp):
    yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    return (last_dailydata_timestamp == None or ts_datetime > last_dailydata_timestamp) and (ts_datetime.day == yesterday.day or ts_datetime < yesterday)

class SolarWeb:
    def __init__(self) -> None:
        self.config = None
        self.last_dailydata_timestamp = None
        self.requests_session = None
        self.pv_system_id = None
        self.sqlcon = None


    def init_dailydata(self):
        self.sqlcon = sqlite3.connect('solarweb.db')
        self.sqlcon.row_factory = sqlite3.Row

        with self.sqlcon:
            cur = self.sqlcon.execute("create table if not exists samples (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")
            cur.execute("create table if not exists daily (id INTEGER PRIMARY KEY AUTOINCREMENT, grid real, solar real, home real, timestamp text)")

        cur = self.sqlcon.cursor()
        for row in cur.execute("SELECT * from daily order by id desc limit 1"):
            self.last_dailydata_timestamp = datetime.datetime.fromisoformat(row["timestamp"])


    def login(self):
        print("Logging into solarweb")
        if self.requests_session != None:
            self.requests_session.close()
        self.requests_session = requests.Session()
        # Get a session
        external_login = self.requests_session.get("https://www.solarweb.com/Account/ExternalLogin")
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
        external_login_callback = self.requests_session.post("https://www.solarweb.com/Account/ExternalLoginCallback", data=commonauth_form_data)
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
        chart_data = self.requests_session.get(f"https://www.solarweb.com/Chart/GetChartNew?pvSystemId={self.pv_system_id}&year={chartday.year}&month={chartday.month}&day={chartday.day}&interval={interval}&view={view}")
        if chart_data.status_code != 200:
            print(chart_data)
            print(chart_data.url)
            print(chart_data.text)
            return None
        return chart_data.json()


    def process_chart_data(self, yesterday):
        # Chart data is a json structure that wraps an array of timestamp / kwh values.
        # The timestamps can be parsed with datetime.datetime.fromtimestamp(val / 1000, tz=datetime.timezone.utc)
        chart_month_production = self.get_chart(yesterday, "month", "production")
        if chart_month_production == None:
            return False

        found_new_data = False
        for data_tuple in chart_month_production["settings"]["series"][0]["data"]:
            ts_datetime = datetime.datetime.fromtimestamp(int(data_tuple[0])/1000, tz=datetime.timezone.utc)
            if is_new_ts(ts_datetime, self.last_dailydata_timestamp):
                found_new_data = True
                break
        if not found_new_data:
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
        for ts,data_dict in daily_data_dict.items():
            ts_datetime = datetime.datetime.fromtimestamp(int(ts)/1000, tz=datetime.timezone.utc)
            if is_new_ts(ts_datetime, self.last_dailydata_timestamp):
                # solar generation = feedin + direct consumption
                # house user = direct consumption + grid
                entry = (ts_datetime.isoformat(), data_dict["grid"], data_dict["direct"] + data_dict["feedin"], data_dict["direct"] + data_dict["grid"])
                with self.sqlcon:
                    self.sqlcon.execute("INSERT INTO daily (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)", entry)
        self.last_dailydata_timestamp = ts_datetime
        return True


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
                pvdata_record = actual_data.json()
                pvdata_record["datetime"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
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

                    with self.sqlcon:
                        self.sqlcon.execute("INSERT INTO samples (timestamp, grid, solar, home) VALUES (?, ?, ?, ?)", 
                            (pvdata_record["datetime"], grid, pv, home))
                else:
                    print(f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} Offline: {json.dumps(pvdata_record)}")
                    sampling_ok = False

                # Get cumulative solar production data for yesterday, this is so that we get
                # full days totals across the month boundary
                yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
                if not self.process_chart_data(yesterday):
                    break

                time.sleep(30)

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
    parser.add_argument('--dump', action='store_true',
                        help='Dump database to stdout and exit')

    args = parser.parse_args()
    if args.history:
        history()
        exit()

    if args.dump:
        con = sqlite3.connect('solarweb.db')
        print("Daily")
        for row in con.execute("SELECT * FROM daily"):
            print(row)
        print("Samples")
        for row in con.execute("SELECT * FROM samples"):
            print(row)
        exit()


    solar_web = SolarWeb()
    solar_web.run()

 
if __name__=="__main__":
    main()