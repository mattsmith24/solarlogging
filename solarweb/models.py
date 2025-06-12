"""Data models for solar data processing."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SolarData:
    """Base class for solar data records."""
    timestamp: datetime
    grid: float
    solar: float
    home: float


@dataclass
class RealtimeData(SolarData):
    """Real-time solar data from the PV system."""
    is_online: bool

