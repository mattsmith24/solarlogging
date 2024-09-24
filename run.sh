#!/bin/bash

cd "$(dirname "$0")"
.venv/bin/python3 solarweb.py --debug --database=solarlogging.db
