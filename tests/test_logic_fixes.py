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

from data_dallion_framework.Common.OrchestrationProcess import BackendSettings
from data_dallion_framework.SilverScripts.SilverLayerProcess import SilverLayerProcess
from data_dallion_framework.GoldScripts.PerformGoldLayerProcess import GoldLayerProcess
from data_dallion_framework.BronzeScripts.PerformBronze import PerformBronze

def test_backend_settings_auto_create():
    settings = BackendSettings(db_auto_create_schema=False)
    assert settings.auto_create_schema == False

def test_empty_dataset_guards(monkeypatch):
    # Mock OrchestrationProcess methods to return empty list
    mock_orch = MagicMock()
    mock_orch.get_dataset_master.return_value = []
    mock_orch.get_ctl_data_acquisition_detail.return_value = []
    
    # Context manager mock
    mock_orch_class = MagicMock()
    mock_orch_class.return_value.__enter__.return_value = mock_orch
    
    monkeypatch.setattr("data_dallion_framework.Common.OrchestrationProcess.OrchestrationProcess", mock_orch_class)
    
    # Verify that initializing these processes does not raise ValueError
    spark_mock = MagicMock()
    
    # Bronze
    bronze = PerformBronze(spark=spark_mock, process_id=999)
    bronze.start_extraction() # should not raise max_workers ValueError
    
    # Silver
    SilverLayerProcess(spark=spark_mock, process_id=999) # should return early without ValueError
    
    # Gold
    GoldLayerProcess(spark=spark_mock, process_id=999) # should return early without ValueError

def test_s3_extractor_timeout_config(monkeypatch):
    mock_boto_client = MagicMock()
    monkeypatch.setattr("boto3.client", mock_boto_client)
    monkeypatch.setattr("pathlib.Path.mkdir", MagicMock())
    
    connection_config_json = '{"client_id": "fake_id", "client_secret": "fake_secret", "region": "us-east-1"}'
    
    try:
        S3Extractor(
            pre_ingestion_logs=[],
            inbound_location="/fake/inbound/",
            outbound_source_location="s3://my-bucket/prefix/",
            file_pattern_static="N",
            file_pattern="*",
            connection_config=connection_config_json,
            pre_ingestion_dataset_id=1,
            process_id=1
        )
    except Exception:
        pass
        
    assert mock_boto_client.called
    args, kwargs = mock_boto_client.call_args
    assert kwargs["aws_access_key_id"] == "fake_id"
    assert kwargs["aws_secret_access_key"] == "fake_secret"
    
    config_obj = kwargs["config"]
    assert config_obj.connect_timeout == 10.0
    assert config_obj.read_timeout == 30.0

from data_dallion_framework.Extractors.SalesforceExtractor import SalesForce

def test_salesforce_query_generator(monkeypatch):
    mock_post = MagicMock()
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"access_token": "fake_token"}
    monkeypatch.setattr("niquests.post", mock_post)
    
    mock_get = MagicMock()
    mock_get.return_value.json.side_effect = [
        {
            "records": [{"Name": "Record 1", "Id": "1"}],
            "done": False,
            "nextRecordsUrl": "/services/data/v62.0/query/next"
        },
        {
            "records": [{"Name": "Record 2", "Id": "2"}],
            "done": True
        }
    ]
    monkeypatch.setattr("niquests.get", mock_get)
    
    sf = SalesForce({"domain": "https://fake.salesforce.com", "client_id": "id", "client_secret": "secret"})
    
    generator = sf.query(columns=["Name", "Id"], dataset_name="Account")
    batches = list(generator)
    
    assert len(batches) == 2
    assert batches[0] == [{"Name": "Record 1", "Id": "1"}]
    assert batches[1] == [{"Name": "Record 2", "Id": "2"}]
