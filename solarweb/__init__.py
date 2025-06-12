"""SolarWeb data collection and processing package.

This package provides functionality to collect and process solar data from SolarWeb.
"""

from .client import SolarWebClient
from .processor import SolarDataProcessor
from .aggregator import (
    DataAggregator,
    FiveMinuteAggregator,
    HourlyAggregator,
    WeeklyAggregator,
    MonthlyAggregator
)

__all__ = [
    'SolarWebClient',
    'SolarDataProcessor',
    'DataAggregator',
    'FiveMinuteAggregator',
    'HourlyAggregator',
    'WeeklyAggregator',
    'MonthlyAggregator'
] 