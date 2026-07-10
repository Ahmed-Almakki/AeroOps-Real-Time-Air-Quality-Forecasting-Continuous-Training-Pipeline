import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, ANY
from src.pipeline.drift_check import model_performance_report

class Test_ModelPerformance:

    @patch('src.pipeline.drift_check.get_run_logger')
    @patch('src.pipeline.drift_check.Report')
    @patch('src.pipeline.drift_check.DataDefinition')
    @patch('src.pipeline.drift_check.Dataset')
    def test_model_performance_logic(self, mock_dataset_class, mock_data_def_class, mock_report_class, mock_get_run_logger):
        mock_schema_instance = MagicMock()
        mock_data_def_class.return_value = mock_schema_instance

        mock_ref_dataset = MagicMock()
        mock_cur_dataset = MagicMock()
        mock_dataset_class.from_pandas.side_effect = [mock_cur_dataset, mock_ref_dataset]

        mock_report_instance = MagicMock()
        mock_report_class.return_value = mock_report_instance

        mock_eval_instance = MagicMock()
        mock_report_instance.run.return_value = mock_eval_instance

        dummy_return_dict = {"tests": "dummy_data"}
        mock_eval_instance.dict.return_value = dummy_return_dict

        dummy_ref = pd.DataFrame({'temp': [20]})
        dummy_cur = pd.DataFrame({'temp': [25]})

        result = model_performance_report.fn(current_data=dummy_cur, refrence_data=dummy_ref)

        mock_data_def_class.assert_called_once_with(
            numerical_columns=ANY,
            categorical_columns=ANY,
            regression=ANY
        )

        calls = mock_dataset_class.from_pandas.call_args_list
        assert len(calls) == 2

        args_1, kwargs_1 = calls[0]
        pd.testing.assert_frame_equal(args_1[0], dummy_cur)
        assert kwargs_1['data_definition'] == mock_schema_instance

        args_2, kwargs_2 = calls[1]
        pd.testing.assert_frame_equal(args_2[0], dummy_ref)
        assert kwargs_2['data_definition'] == mock_schema_instance


        # mock_report_class.assert_called_once_with(
        #     ANY,
        #     include_tests=True
        # )

        # assert mock_dataset_class.from_pandas.call_count == 2
        # mock_dataset_class.from_pandas.assert_any_call(dummy_cur, data_definition=mock_schema_instance)
        # mock_dataset_class.from_pandas.assert_any_call(dummy_ref, data_definition=mock_schema_instance)

        mock_report_instance.run.assert_called_once_with(
            reference_data=mock_ref_dataset,
            current_data=mock_cur_dataset
        )

        assert mock_eval_instance.save_html.call_count == 1
        assert result == dummy_return_dict
