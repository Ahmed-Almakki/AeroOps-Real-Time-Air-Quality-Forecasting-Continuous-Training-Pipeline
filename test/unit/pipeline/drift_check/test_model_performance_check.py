import pytest
from unittest.mock import MagicMock, patch
from src.pipeline.drift_check import model_performance_check


class Test_ModelPerformance:

    @patch('src.pipeline.drift_check.get_run_logger')
    def test_model_performance_check(self, mock_logger_instance):
        mock_logger = mock_logger_instance.return_value

        test = { 'tests' : [
                {'name': 'placeholder',
                 'status': "FAIL"
                }
            ]
        }

        result = model_performance_check.fn(test)

        assert result == True
        assert mock_logger.info.call_count == 1
        assert mock_logger.warning.call_count == 2

    @patch('src.pipeline.drift_check.get_run_logger')
    def test_model_performance_check_no_failing_test(self, mock_logger_instance):
        mock_logger = mock_logger_instance.return_value

        test = { 'tests' : [
                {'name': 'placeholder',
                 'status':  "No_Fail"
                }
            ]
        }

        result = model_performance_check.fn(test)

        assert result == False
        assert mock_logger.info.call_count == 2
        assert mock_logger.warning.call_count == 0

    @patch('src.pipeline.drift_check.get_run_logger')
    def test_model_performance_check_missing_test(self, mock_logger_instance):
        mock_logger = mock_logger_instance.return_value

        test = {}
        with pytest.raises(KeyError):
            model_performance_check.fn(test)

        assert mock_logger.error.called
