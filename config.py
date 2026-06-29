"""
Configuration for Ceph S3 Load Testing Tool.
All settings can be overridden via environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Flask Configuration
FLASK_ENV = os.getenv('FLASK_ENV', 'production')
DEBUG = FLASK_ENV == 'development'
HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', 5000))

# AWS/Ceph Configuration
AWS_REGION = os.getenv('AWS_REGION', 'default')
AWS_ENDPOINT_URL = os.getenv('AWS_ENDPOINT_URL', None)
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', None)
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', None)

# Test Configuration
DEFAULT_SCENARIO = os.getenv('DEFAULT_SCENARIO', 'mixed_7030')
MAX_TIMESERIES_POINTS = int(os.getenv('MAX_TIMESERIES_POINTS', 3600))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 300))

# Reports Configuration
REPORTS_DIR = os.getenv('REPORTS_DIR', 'reports')

# Logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'ceph_load_test.log')

# UI Configuration
METRICS_POLL_INTERVAL = int(os.getenv('METRICS_POLL_INTERVAL', 1000))  # milliseconds

# Performance
MAX_CONCURRENT_OPERATIONS = int(os.getenv('MAX_CONCURRENT_OPS', 256))
