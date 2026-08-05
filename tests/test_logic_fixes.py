from data_dallion_framework.Common.FileNameGenerator import file_name_generator
from data_dallion_framework.Common.PatternValidator import validate_pattern
from data_dallion_framework.Extractors.S3Extractor import S3Extractor
from unittest.mock import MagicMock

def test_file_name_generator_multiple_replacements():
    # Verify that both YYYYMM and YYYY are replaced when present
    # (or YYYYMMDD and YYYY)
    filename = "report_YYYYMM_data_YYYY.csv"
    res = file_name_generator(filename)
    assert "YYYYMM" not in res
    assert "YYYY" not in res
    # Should contain digits
    assert len(res) == len(filename) - 6 - 4 + 6 + 4 # length should match since format replaces same-length chars

def test_pattern_validator_dot_escaping_and_fullmatch():
    # Verify dot matches literal dot, not any char
    # "report_*.csv" should NOT match "report_123_csv" or "report_123.csv.temp"
    assert validate_pattern("report_*.csv", "report_123.csv") == True
    assert validate_pattern("report_*.csv", "report_123_csv") == False
    assert validate_pattern("report_*.csv", "report_123.csv.temp") == False

def test_s3_parse_location_uri_schemes():
    # Create mock S3Extractor to call parse_location without running __init__
    extractor = object.__new__(S3Extractor)
    
    # Test standard s3:// URI
    res = extractor.parse_location("s3://my-bucket/some/folder/")
    assert res["Bucket"] == "my-bucket"
    assert res["Prefix"] == "some/folder/"

    # Test s3a:// URI
    res = extractor.parse_location("s3a://another-bucket/nested/dir/")
    assert res["Bucket"] == "another-bucket"
    assert res["Prefix"] == "nested/dir/"

    # Test no-scheme location
    res = extractor.parse_location("my-simple-bucket/folder")
    assert res["Bucket"] == "my-simple-bucket"
    assert res["Prefix"] == "folder/"
