import pandas as pd
import pytest
from truflation.data.economic_data_ingestor import EconomicDataIngestor

# Test data for a CSV file with economic data.
TEST_CSV_DATA = """
Date,Value
2021-01-01,100
2021-02-01,200
2021-03-01,
"""


@pytest.fixture
def sample_csv(tmp_path):
    # Create a temporary CSV file with test data.
    csv_file = tmp_path / "cci.csv"
    csv_file.write_text(TEST_CSV_DATA.strip())
    return str(csv_file)


def test_read_data_success(sample_csv):
    # Test the read_data method for success.
    ingestor = EconomicDataIngestor(sample_csv)
    df = ingestor.read_data()
    assert not df.empty, "DataFrame should not be empty"
    assert list(df.columns) == [
        "Date",
        "Value",
    ], "DataFrame should have Date and Value columns"
    assert len(df) == 3, "DataFrame should have three rows"


def test_process_data(sample_csv):
    # Test the process_data method.
    ingestor = EconomicDataIngestor(sample_csv)
    df = ingestor.read_data()
    processed_df = ingestor.process_data(df)
    assert (
        not processed_df.isnull().values.any()
    ), "Processed DataFrame should not contain null values"
    assert len(processed_df) < len(
        df
    ), "Processed DataFrame should have fewer rows than the original due to dropped nulls"


def test_read_data_file_not_found():
    # Test the read_data method when the file is not found.
    ingestor = EconomicDataIngestor("nonexistent_file.csv")
    with pytest.raises(Exception) as exc_info:
        ingestor.read_data()
    assert "The file nonexistent_file.csv was not found" in str(
        exc_info.value
    ), "An appropriate FileNotFoundError should be raised"


def test_read_data_empty_file(tmp_path):
    # Test the read_data method with an empty file.
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("")
    ingestor = EconomicDataIngestor(str(empty_file))
    with pytest.raises(Exception) as exc_info:
        ingestor.read_data()
    assert "No data found in the file" in str(
        exc_info.value
    ), "An appropriate EmptyDataError should be raised"


def test_read_data_general_exception(tmp_path):
    # Test the read_data method for a general exception (e.g., bad CSV formatting).
    bad_csv_file = tmp_path / "bad_format.csv"
    bad_csv_file.write_text("Date;Value\n2021-01-01;100")
    ingestor = EconomicDataIngestor(str(bad_csv_file))
    with pytest.raises(Exception) as exc_info:
        ingestor.read_data()
    assert "An error occurred while reading the file" in str(
        exc_info.value
    ), "A general Exception should be raised for other errors"
