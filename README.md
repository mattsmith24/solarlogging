# pictureframe

A python program that logs the status of a Fronius PV inverter to a sqlite DB.

Required python modules:

- requests
- beautifulsoup4

Configuration is via a solarweb.json file with the following format:

```json
{
  "username": "your solarweb username (email)",
  "password": "your solarweb password"
}
```

To run it, try `python solarweb.py`.

There is an optional argument to download solar history to the daily table in the DB.
To use this, first add a field 'install_date' to the solarweb.json file with format 
'YYYY-MM-DD'. The script will attempt to download daily data from the date until now.
To run it type `python solarweb.py --history`.

For debugging, run with `python solarweb.py --debug`. This will print messages about
the data that is being inserted to the database.
