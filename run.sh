#!/bin/bash

cd "$(dirname "$0")"
/opt/solarlogging/uv run python -m solarweb.main --debug --database=solarlogging.db
