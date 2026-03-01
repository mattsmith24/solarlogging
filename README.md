# Solar Logging

A python program that logs the status of a Fronius PV inverter to a sqlite DB.

Configuration is via a solarweb.json file with the following format:

```json
{
  "username": "your solarweb username (email)",
  "password": "your solarweb password"
}
```

To run it, try `python -m solarweb.main`.

The sqlite3 database will be saved as solarlogging.db in the user data directory.
This location is printed at app startup.

There is an optional argument to download solar history to the daily table in the DB.
To use this, first add a field 'install_date' to the solarweb.json file with format 
'YYYY-MM-DD'. The script will attempt to download daily data from the date until now.
To run it type `python -m solarweb.main --history`.

For debugging, run with `python -m solarweb.main --debug`. This will print messages about
the data that is being inserted to the database.

# Install

Install uv https://docs.astral.sh/uv/

Then run it, use `uv run python -m solarweb.main`. uv will create a venv and
install dependendencies on first run.

# Deploy

Copy files to /opt/solarlogging

```
sudo useradd -r -d /opt/solarlogging solarlogging
sudo chown -R solarlogging /opt/solarlogging
sudo cp solarlogging.service to /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl enable solarlogging
sudo systemctl start solarlogging
```

