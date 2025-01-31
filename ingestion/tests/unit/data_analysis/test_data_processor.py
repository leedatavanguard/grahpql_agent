"""Unit tests for the DataProcessor class."""

import pytest
import json
import time
from pathlib import Path
from ingestion.utils.data_analysis.data_processor import DataProcessor
from ingestion.utils.data_analysis.errors import CircularReferenceError, InvalidDataStructureError

@pytest.fixture
def test_data():
    """Load test data."""
    return {
        "data": {
            "clubMemberships": [  # Note: Changed to a list of memberships
                {
                    "id": "31_130029",
                    "status": "ACTIVE",
                    "expiryDate": "2025-12-31",
                    "athlete": {
                        "id": "130029",
                        "firstName": "John",
                        "lastName": "Doe",
                        "properties": {
                            "age": 25,
                            "address": {
                                "city": "Sydney",
                                "state": "NSW"
                            }
                        }
                    },
                    "club": {
                        "id": "31",
                        "name": "Bondi Surf Club",
                        "location": {
                            "latitude": -33.8915,
                            "longitude": 151.2767
                        }
                    }
                }
            ]
        }
    }

def test_flatten_club_membership(test_data):
    """Test flattening of club membership data."""
    processor = DataProcessor()
    flattened, metadata = processor.flatten_data(test_data)
    
    # Check basic structure
    assert len(flattened) == 1
    assert metadata['record_count'] == 1
    assert 'data.clubMemberships' in metadata['main_path']
    
    # Check flattened fields
    record = flattened[0]
    assert record['id'] == '31_130029'
    assert record['status'] == 'ACTIVE'
    assert record['athlete_firstName'] == 'John'
    assert record['athlete_properties_address_city'] == 'Sydney'
    assert record['club_location_latitude'] == -33.8915
    
    # Check metadata
    fields = metadata['fields']
    assert 'data.clubMemberships.id' in fields
    assert fields['data.clubMemberships.status']['type'] == 'str'
    assert fields['data.clubMemberships.club.location.latitude']['type'] == 'float'

def test_circular_reference_detection():
    """Test detection of circular references."""
    # Create a list structure with a circular reference
    circular_data = {
        "data": {
            "items": [
                {
                    "id": "1",
                    "nested": {}
                }
            ]
        }
    }
    circular_data['data']['items'][0]['nested']['circular'] = circular_data
    
    processor = DataProcessor()
    with pytest.raises(CircularReferenceError):
        processor.flatten_data(circular_data)

def test_nested_list_handling():
    """Test handling of nested lists and complex structures."""
    test_data = {
        "data": {
            "athletes": [
                {
                    "id": "130029",
                    "firstName": "John",
                    "lastName": "Doe",
                    "certifications": [
                        {"name": "First Aid", "expiry": "2025-01-01"},
                        {"name": "CPR", "expiry": "2025-02-01"}
                    ]
                }
            ]
        }
    }
    
    processor = DataProcessor()
    flattened, metadata = processor.flatten_data(test_data)
    
    # Verify structure
    assert len(flattened) == 1
    record = flattened[0]
    assert record['id'] == '130029'
    assert record['firstName'] == 'John'
    
    # Check that nested list paths are captured
    paths = metadata['fields'].keys()
    assert any('certifications' in path for path in paths)
    
    # Check statistics
    cert_paths = [path for path in paths if 'certifications' in path]
    assert len(cert_paths) > 0  # Should have certification-related paths

def test_large_dataset_handling():
    """Test processing of large datasets with sampling."""
    # Generate large nested dataset
    large_data = {
        "data": {
            "items": [
                {
                    "id": str(i),
                    "nested": {
                        "field1": f"value{i}",
                        "field2": i,
                        "deep": {"more": "data"}
                    }
                } for i in range(1000)  # Reduced from 10000 for test speed
            ]
        }
    }
    
    processor = DataProcessor(sample_size=100)
    start_time = time.time()
    flattened, metadata = processor.flatten_data(large_data)
    processing_time = time.time() - start_time
    
    assert len(flattened) == 1000
    assert processing_time < 5.0  # Should process within reasonable time
    assert metadata["record_count"] == 1000
    assert len(metadata["fields"]) > 0
    # Verify sampling worked by checking field stats
    assert metadata["fields"]["data.items.id"]["total_count"] <= 100

def test_type_consistency():
    """Test handling of mixed types and type inference."""
    data = {
        "data": {
            "items": [
                {
                    "id": 1,  # integer
                    "id_str": "1",  # string
                    "active": True,  # boolean
                    "score": 75.5,  # float
                    "mixed": "123"  # string that looks like number
                },
                {
                    "id": "2",  # string instead of integer
                    "id_str": 2,  # integer instead of string
                    "active": 1,  # number instead of boolean
                    "score": "89.5",  # string instead of float
                    "mixed": 123  # number instead of string
                }
            ]
        }
    }
    
    processor = DataProcessor()
    flattened, metadata = processor.flatten_data(data)
    
    # Check type information in metadata
    id_field = metadata["fields"]["data.items.id"]
    assert len(id_field["unique_values"]) == 2
    assert id_field["type"] in ["str", "int"]
    
    # Check value preservation
    assert len(flattened) == 2
    assert all("items_id" in item for item in flattened)
    assert all("items_score" in item for item in flattened)
    assert all("items_mixed" in item for item in flattened)

# Commented out tests
# def test_flatten_club_membership_new():
#     """Test flattening of club membership data."""
#     data = {
#         "data": {
#             "items": [
#                 {
#                     "id": "1",
#                     "name": "Test Club",
#                     "members": [
#                         {"id": "m1", "name": "Member 1"},
#                         {"id": "m2", "name": "Member 2"}
#                     ]
#                 }
#             ]
#         }
#     }
    
#     processor = DataProcessor()
#     flattened, metadata = processor.flatten_data(data)
    
#     assert len(flattened) == 1
#     assert flattened[0]["items_id"] == "1"
#     assert flattened[0]["items_name"] == "Test Club"
#     assert isinstance(metadata, dict)
#     assert "fields" in metadata

# def test_circular_reference_detection_new():
#     """Test detection of circular references."""
#     # Create a list structure with a circular reference
#     circular_data = {
#         "data": {
#             "items": [
#                 {
#                     "id": "1",
#                     "nested": {}
#                 }
#             ]
#         }
#     }
#     circular_data['data']['items'][0]['nested']['circular'] = circular_data
    
#     processor = DataProcessor()
#     with pytest.raises(CircularReferenceError):
#         processor.flatten_data(circular_data)

# def test_nested_list_handling_new():
#     """Test handling of nested lists."""
#     data = {
#         "data": {
#             "items": [
#                 {
#                     "id": "1",
#                     "tags": ["tag1", "tag2"],
#                     "details": {
#                         "scores": [1, 2, 3]
#                     }
#                 }
#             ]
#         }
#     }
    
#     processor = DataProcessor()
#     flattened, metadata = processor.flatten_data(data)
    
#     assert len(flattened) == 1
#     assert flattened[0]["items_id"] == "1"
#     assert isinstance(metadata["fields"], dict)

# def test_international_character_handling():
#     """Test handling of international characters in both keys and values."""
#     data = {
#         "data": {
#             "items": [
#                 {
#                     "id": "1",
#                     "日本語": "テスト",  # Japanese characters in key
#                     "mixed_field": "English と 日本語",  # Mixed characters
#                     "special_chars": "®™€²³¾",  # Special characters
#                     "emoji": "🌟🎉🎊"  # Emoji
#                 }
#             ]
#         }
#     }
    
#     processor = DataProcessor()
#     flattened, metadata = processor.flatten_data(data)
    
#     assert "items_日本語" in flattened[0]
#     assert flattened[0]["items_日本語"] == "テスト"
#     assert metadata["fields"]["data.items.日本語"]["contains_unicode"] is True
#     assert "Lo" in metadata["fields"]["data.items.日本語"]["unicode_categories"]  # Lo = Letter, other
#     assert metadata["encoding_recommendations"]["data.items.日本語"] == "utf-8"

# def test_sparse_data_handling():
#     """Test handling of sparse data with missing fields."""
#     data = {
#         "data": {
#             "items": [
#                 {"id": "1", "field1": "value1", "field2": "value2"},
#                 {"id": "2", "field1": "value3"},  # field2 missing
#                 {"id": "3", "field2": "value4"},  # field1 missing
#                 {"id": "4", "nested": {"deep": "value"}},  # different structure
#                 {}  # empty record
#             ]
#         }
#     }
    
#     processor = DataProcessor()
#     flattened, metadata = processor.flatten_data(data)
    
#     assert len(flattened) == 5
#     # Check null counts in metadata
#     field1_stats = metadata["fields"]["data.items.field1"]
#     field2_stats = metadata["fields"]["data.items.field2"]
#     assert field1_stats["null_count"] >= 2  # At least 2 nulls
#     assert field2_stats["null_count"] >= 2  # At least 2 nulls
#     # Verify nulls in flattened data
#     assert sum(1 for item in flattened if item.get("items_field1") is None) >= 2
#     assert sum(1 for item in flattened if item.get("items_field2") is None) >= 2

# def test_multiple_list_detection():
#     """Test correct main list detection when multiple lists exist."""
#     data = {
#         "metadata": {
#             "tags": ["tag1", "tag2"]  # Secondary list
#         },
#         "data": {
#             "items": [  # Main list
#                 {"id": "1", "value": "test1"},
#                 {"id": "2", "value": "test2"}
#             ],
#             "related": [  # Another secondary list
#                 {"ref": "ref1"},
#                 {"ref": "ref2"}
#             ]
#         }
#     }
    
#     processor = DataProcessor()
#     flattened, metadata = processor.flatten_data(data)
    
#     assert metadata["main_path"] == "data.items"
#     assert len(flattened) == 2
#     assert all("items_id" in item for item in flattened)
#     assert all("items_value" in item for item in flattened)

if __name__ == '__main__':
    pytest.main([__file__])
