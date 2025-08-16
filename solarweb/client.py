"""Client for interacting with the SolarWeb API."""

import time
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from .models import RealtimeData


class SolarWebClient:
    """Client for interacting with the SolarWeb API.
    
    This class handles all direct interactions with the SolarWeb website including:
    - Authentication
    - Fetching real-time data
    - Fetching historical chart data
    """
    
    def __init__(self, config, debug=False):
        """Initialize SolarWeb client.
        
        Args:
            config: Configuration dictionary containing username and password
            debug: Enable debug logging
        """
        self.config = config
        self.debug_enabled = debug
        self.requests_session = None
        self.pv_system_id = None
        self.last_login_attempt = None

    def debug(self, msg):
        """Log debug message if debug mode is enabled."""
        if self.debug_enabled:
            print(msg)

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
        
        # Set a realistic User-Agent to avoid being blocked
        self.requests_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        try:
            # Get initial session
            self.debug("Getting initial session")
            external_login = self.requests_session.get("https://www.solarweb.com/Account/ExternalLogin")
            if external_login.status_code != 200:
                print("Error: Failed to access ExternalLogin")
                self.debug(external_login)
                self.debug(external_login.url)
                self.debug(external_login.text)
                return False

            # Parse session data key
            self.debug("Parsing session data key")
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
            self.debug("Posting to commonauth")
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
            self.debug("Posting to external login callback")
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
        """Get chart data from SolarWeb. The charts API is not documented but
        appears to provide daily consumption and production data for a given
        month. The chartday is the date to get data for. The interval is the
        data interval (e.g. 'month') and the view is the data view type (e.g.
        'production', 'consumption').
        
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

    def get_realtime_data(self):
        """Get real-time solar data from SolarWeb. The data is not documented
        but appears to be the current state of the PV system updated every 30
        seconds.
        
        Returns:
            RealtimeData: Real-time data or None if request failed
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
                
            data = actual_data.json()
            if not data:
                return None
                
            # Extract values with defaults
            grid = data.get('P_Grid', 0) or 0
            pv = data.get('P_PV', 0) or 0
            home = -(data.get('P_Load', 0) or 0)
            is_online = data.get('IsOnline', False)
            
            return RealtimeData(
                timestamp=datetime.now(tz=timezone.utc),
                grid=grid,
                solar=pv,
                home=home,
                is_online=is_online
            )
            
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
        return self.login()

    def close(self):
        """Close the client session."""
        if self.requests_session is not None:
            self.requests_session.close() 