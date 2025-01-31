"""Constants for the exploration module.

This module defines constants used across the exploration package,
particularly for file naming, output organization, and analysis parameters.
"""

from pathlib import Path

# Output file names
RAW_DATA_FILE = "raw_data.json"
FLAT_DATA_FILE = "flat_data.json"
DATA_REPORT_FILE = "data_report.md"

# Sampling parameters for development
DEV_MODE = True  # Set to False for production
SAMPLE_SIZES = {
    'dev': 10,      # Development mode sample size
    'test': 100,    # Testing mode sample size
    'prod': None    # Production mode - use all records
}
SAMPLE_SIZE = SAMPLE_SIZES['dev'] if DEV_MODE else SAMPLE_SIZES['prod']

# Analysis thresholds
MIN_RECORDS_FOR_SAMPLING = 1000  # Only sample if we have more than this many records
MAX_UNIQUE_VALUES = 100  # Maximum unique values to track per field
NULL_THRESHOLD_PCT = 20  # Alert if null percentage exceeds this
MAX_NESTED_DEPTH = 10  # Maximum depth for nested structure analysis

# Report sections
REPORT_SECTIONS = {
    'SCHEMA': 'Schema Analysis',
    'DATA_QUALITY': 'Data Quality',
    'TRANSFORMATION': 'Transformation Rules',
    'IMPLEMENTATION': 'Implementation Notes'
}

# Date formats
TIMESTAMP_FORMAT = "%d_%H%M%S"
DATE_FORMAT = "%Y_%m_%d"
SUPPORTED_DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y"
]

# Output directory structure
OUTPUT_DIR = Path(__file__).parent.parent.parent.parent / "exploration_outputs"
SCHEMA_ANALYSIS_TEMPLATE = "schema_analysis_{date}.md"
DATA_QUALITY_TEMPLATE = "data_quality_report_{date}.md"
TRANSFORMATION_TEMPLATE = "transformation_rules_{date}.md"
IMPLEMENTATION_TEMPLATE = "implementation_guide_{date}.md"

# Data type inference
TYPE_INFERENCE_SAMPLE_SIZE = 100  # Number of values to check for type inference
NUMERIC_THRESHOLD_PCT = 90  # Percentage of numeric values needed to classify as numeric
DATE_THRESHOLD_PCT = 90  # Percentage of date values needed to classify as date
