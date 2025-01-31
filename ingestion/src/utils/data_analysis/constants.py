"""Constants for data analysis.

This module contains configuration constants used across the data analysis package.
Using constants instead of magic numbers improves code readability and maintainability.
"""

# Maximum number of list items to sample when analyzing structure
MAX_LIST_SAMPLE_SIZE = 10

# Maximum number of unique values to store in field statistics
MAX_UNIQUE_VALUES = 100

# Minimum sample size for type inference
MIN_TYPE_INFERENCE_SAMPLE = 5

# Date format patterns for type inference
DATE_FORMATS = [
    '%Y-%m-%d',
    '%d/%m/%Y',
    '%m/%d/%Y',
    '%Y/%m/%d'
]

# Boolean string representations
BOOLEAN_TRUE_VALUES = {'true', 'yes', '1', 't', 'y'}
BOOLEAN_FALSE_VALUES = {'false', 'no', '0', 'f', 'n'}
