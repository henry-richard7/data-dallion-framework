# constants.py
from datetime import date

# System-wide date constants
HIGH_DATE_STR = "9999-12-31"
HIGH_DATE_ISO = date(9999, 12, 31)

# Status indicators
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"
STATUS_COMPLETED = "COMPLETED"

# Table types
TABLE_TYPE_EXTERNAL = "external"
TABLE_TYPE_MANAGED = "managed"

# Criticality levels
CRITICALITY_CRITICAL = "C"
CRITICALITY_WARNING = "W"

# Default API Configuration
API_DEFAULT_TIMEOUT = 30 # seconds
API_DEFAULT_RETRIES = 3
API_BACKOFF_FACTOR = 0.5
