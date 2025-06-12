#!/bin/bash

cd "$(dirname "$0")"
.venv/bin/python3 -m solarweb.main --debug --database=solarlogging.db
