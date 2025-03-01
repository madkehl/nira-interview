import pandas as pd
from unittest.mock import patch
from pipeline.interview_job.ops.get_existing_bus_info import get_existing_bus_info


@patch('pandas.read_csv')
@patch('os.path.exists')
def test_bus_info_returns_bus_number(mock_exists, mock_read_csv):
    mock_exists.return_value = True
    # Create a mock DataFrame
    mock_df = pd.DataFrame({
        'bus_number': ['1a', '2a', '3a'],
        'mw_available': [50000, 100000, 150000],
        'gw_available': [5, 10, 15],
    })
    # Set the mock to return the mock DataFrame when called
    mock_read_csv.return_value = mock_df
    # Call the function that reads the CSV (it will use the mocked read_csv)
    result = get_existing_bus_info('mock_file.csv')
    # Expected result
    expected_result = ['1a', '2a', '3a']
    # Assert that the result is as expected
    assert result == expected_result


@patch('pandas.read_csv')
@patch('os.path.exists')
def test_bus_info_returns_empty_if_no_path(mock_exists, mock_read_csv):
    mock_exists.return_value = False
    # Create a mock DataFrame
    mock_df = pd.DataFrame({
        'bus_number': ['1a', '2a', '3a'],
        'mw_available': [50000, 100000, 150000],
        'gw_available': [5, 10, 15],
    })
    # Set the mock to return the mock DataFrame when called
    mock_read_csv.return_value = mock_df
    # Call the function that reads the CSV (it will use the mocked read_csv)
    result = get_existing_bus_info('mock_file.csv')
    # Expected result
    expected_result = []
    # Assert that the result is as expected
    assert result == expected_result
