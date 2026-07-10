import pytest
from unittest.mock import MagicMock, patch
from prefect import get_run_logger

from src.pipeline.drift_check import check_for_drift


class Test_dataDrift:

    @patch('src.pipeline.drift_check.get_run_logger')
    def test_check_for_drift(self, mock_logger_instance):
        report = { "metrics" : [
            {'value': {'share': 0.6}},
            {'config': {
                'type': 'evidently:metric_v2:ValueDrift',
                'column': 'placeholder',
                'method': 'method_placholder',
                'threshold': 1},
            'value': 1
            }]
        }

        mock_logger = mock_logger_instance.return_value

        report['metrics'][1]['config']['threshold'] = 2

        result = check_for_drift.fn(report)

        assert result == True
        assert mock_logger.warning.call_count == 2

    @patch('src.pipeline.drift_check.get_run_logger')
    def test_check_for_drift_bad_request(self, mock_logger_instance):
        report = { "metrics" : [
            {'value': {'share': 0.6}},
            ]
        }

        mock_logger = mock_logger_instance.return_value

        result = check_for_drift.fn(report)

        assert result is True
        assert mock_logger.warning.call_count == 1


    @patch('src.pipeline.drift_check.get_run_logger')
    def test_check_for_drift_missing_values(self, mock_logger_instance):
        report = {}

        mock_logger = mock_logger_instance.return_value

        with pytest.raises(KeyError):
            check_for_drift.fn(report)

        assert mock_logger.error.called
