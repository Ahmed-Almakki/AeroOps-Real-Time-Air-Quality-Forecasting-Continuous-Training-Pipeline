import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from src.data_prep.process_input import process_input


def test_process_input_happy_path():
    """ Everything work perfectly"""
    input_data = {
        "payload": {
            "after": {
                "No": 1,
                "station": "A1",
                "temperature": 25.5,
                "wd": "ENE"
            }
        }
    }

    predicted_output = process_input(input_data)

    expected_data = {
        "temperature": [25.5],
        "wd_ENE": [True]
    }
    expected_output = pd.DataFrame(expected_data)

    assert_frame_equal(predicted_output, expected_output, check_like=True)


def test_process_input_missing_drop_columns():
    """ Missing 'No' and 'station' """
    input_data = {
        "payload": {
            "after": {
                "temperature": 25.5,
                "wd": "E"
            }
        }
    }

    predicted_output = process_input(input_data)

    expected_data = {
        "temperature": [25.5],
        "wd_E": [True]
    }
    expected_output = pd.DataFrame(expected_data)

    assert_frame_equal(predicted_output, expected_output, check_like=True)


def test_process_input_missing_wd_column():
    """ Missing 'wd' column """
    input_data = {
        "payload": {
            "after": {
                "No": 2,
                "station": "B2",
                "temperature": 30.0
            }
        }
    }

    predicted_output = process_input(input_data)


    expected_output = pd.DataFrame({"temperature": [30.0]})

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
