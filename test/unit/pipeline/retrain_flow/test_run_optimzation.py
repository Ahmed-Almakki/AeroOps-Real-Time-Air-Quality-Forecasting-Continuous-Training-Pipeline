import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.pipeline.retrain_flow import run_optmization


class Test_RunOptmization:
    @patch("src.pipeline.retrain_flow.fmin")
    @patch("src.pipeline.retrain_flow.Trials")
    @patch("src.pipeline.retrain_flow.get_run_logger")
    def test_run_optmization_happy_path(self, mock_get_run_logger, mock_trials, mock_fmin):
        dummy_x = pd.DataFrame({"feature_1": [1.0, 2.0], "feature_2": [3.0, 4.0]})
        dummy_y = pd.DataFrame({"target": [10.5, 20.1]})

        mock_trials_instance = MagicMock()
        mock_trials.return_value = mock_trials_instance

        # Beautifully bypassing the Prefect decorator here!
        run_optmization.fn(
            X_train=dummy_x,
            Y_train=dummy_y,
            X_test=dummy_x,
            Y_test=dummy_y,
            num_trials=10
        )

        mock_fmin.assert_called_once()

        _, kwargs = mock_fmin.call_args

        assert kwargs["max_evals"] == 10
        assert kwargs["trials"] == mock_trials_instance
        assert callable(kwargs["fn"])

        mock_logger = mock_get_run_logger.return_value
        mock_logger.info.assert_any_call("Starting Processing data...")
        mock_logger.info.assert_any_call("Optimization completed with %s trials.", 10)

    @patch("src.pipeline.retrain_flow.get_run_logger")
    def test_run_optimization_raises_value_error_on_empty_data(self, mock_get_run_logger):
        mock_logger = mock_get_run_logger.return_value

        empty_df = pd.DataFrame()
        valid_df = pd.DataFrame({"feature": [1, 2, 3]})

        with pytest.raises(ValueError, match="missing either train/test feature or train/test output"):
            run_optmization.fn(
                X_train=empty_df,
                Y_train=valid_df,
                X_test=valid_df,
                Y_test=valid_df
            )
