import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

# Note: Make sure to import EXPECTED_FEATURES from your source file
from src.data_prep.process_input import process_input, EXPECTED_FEATURES


def test_process_input_happy_path():
    """ Everything works perfectly """
    input_data = {
        "payload": {
            "after": {
                "No": 1,
                "temp": 25.5  # Changed 'temperature' to 'temp'
            }
        }
    }

    predicted_output = process_input(input_data)

    expected_data = {feature: [0] for feature in EXPECTED_FEATURES}
    expected_data["temp"] = [25.5]
    expected_output = pd.DataFrame(expected_data)

    assert_frame_equal(predicted_output, expected_output, check_like=True)


def test_process_input_missing_drop_columns():
    """ Missing 'No' and 'station' """
    input_data = {
        "payload": {
            "after": {
                "temp": 25.5,
                "rain": 1.0
            }
        }
    }

    predicted_output = process_input(input_data)

    expected_data = {feature: [0] for feature in EXPECTED_FEATURES}
    expected_data["temp"] = [25.5]
    expected_data["rain"] = [1.0]
    expected_output = pd.DataFrame(expected_data)

    assert_frame_equal(predicted_output, expected_output, check_like=True)


def test_process_input_missing_wd_column():
    """ Missing 'wd' column """
    input_data = {
        "payload": {
            "after": {
                "No": 2,
                "station": "B2",
                "temp": 30.0
            }
        }
    }

    predicted_output = process_input(input_data)

    expected_data = {feature: [0] for feature in EXPECTED_FEATURES}
    expected_data["temp"] = [30.0]
    expected_output = pd.DataFrame(expected_data)

    assert_frame_equal(predicted_output, expected_output, check_like=True)


def test_process_input_empty_after_data():
    """ Empty 'after' data """
    input_data = {
        "payload": {
            "after": {}
        }
    }

    predicted_output = process_input(input_data)
    expected_output = pd.DataFrame()

    assert_frame_equal(predicted_output, expected_output)


def test_process_input_missing_payload_key():
    """ Bad Data Structure """
    input_data = {"wrong_key": "bad_data"}

    with pytest.raises(Exception):
        process_input(input_data)
