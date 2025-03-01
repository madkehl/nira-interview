import pandas as pd
from unittest.mock import patch
from pipeline.interview_job.ops.raw_buses_to_run import raw_buses_to_run


@patch('pipeline.interview_job.ops.raw_buses_to_run._RAW_CSV_TO_READ', 'mocked/path.csv')
@patch('pandas.read_csv')
def test_raw_buses_to_run_with_ls(mock_read_csv):
    # Create a mock DataFrame to simulate CSV data
    mock_df = pd.DataFrame({
        'bus_number': ['1a', '2a', '3a'],
        'mw_available': [5000, 10000, 15000],
        'gw_available': [5, 10, 15],
    })
    mock_read_csv.return_value = mock_df

    # Test 1: Some buses have not run yet
    ran_prev = ['1a']
    result = raw_buses_to_run(ran_prev)

    # Expected: Filter out '1a'
    expected_result = pd.DataFrame({
        'bus_number': ['2a', '3a'],
        'mw_available': [10000, 15000],
        'gw_available': [10, 15],
    })
    pd.testing.assert_frame_equal(result, expected_result)


@patch('pipeline.interview_job.ops.raw_buses_to_run._RAW_CSV_TO_READ', 'mocked/path.csv')
@patch('pandas.read_csv')
def test_raw_buses_to_run_without_ls(mock_read_csv):
    # Create a mock DataFrame to simulate CSV data
    mock_df = pd.DataFrame({
        'bus_number': ['1a', '2a', '3a'],
        'mw_available': [50000, 100000, 150000],
        'gw_available': [5, 10, 15],
    })
    mock_read_csv.return_value = mock_df

    # Test 1: Some buses have not run yet
    ran_prev = []
    result = raw_buses_to_run(ran_prev)

    pd.testing.assert_frame_equal(result, mock_df)
