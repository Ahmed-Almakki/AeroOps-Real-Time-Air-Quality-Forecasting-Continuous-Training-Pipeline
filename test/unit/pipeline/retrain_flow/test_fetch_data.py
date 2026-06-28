import pytest
from unittest.mock import patch, MagicMock

from src.pipeline.retrain_flow import fetch_data


class Test_FetchData:

    @patch('src.pipeline.retrain_flow.os.getenv')
    @patch('src.pipeline.retrain_flow.create_engine')
    @patch('src.pipeline.retrain_flow.Session')
    @patch('src.pipeline.retrain_flow.get_run_logger')
    def test_fetch_data(self, mock_logger_instance, mock_session, mock_engine, mock_getenv):
        mock_logger = mock_logger_instance.return_value
        mock_getenv.side_effect = ['ahmed', '123', 'host', 'db', 'table_name']

        mock_result = MagicMock()

        dummy_query_result = [(i, "a"*i) for i in range(10)]
        mock_result.fetchall.return_value = dummy_query_result
        mock_result.keys.return_value = ['id', 'name']
        mock_session_instance = MagicMock()
        mock_session_instance.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_session_instance

        train_df, test_df = fetch_data.fn()

        assert mock_logger.info.call_count == 1
        assert len(test_df) == 10 * 0.1
        assert len(train_df) == 10 - (10 * 0.1)
        assert list(train_df.columns) == ['id', 'name']

        mock_engine.assert_called_once_with('postgresql+psycopg://ahmed:123@host/db')


    @patch('src.pipeline.retrain_flow.os.getenv')
    @patch('src.pipeline.retrain_flow.create_engine')
    @patch('src.pipeline.retrain_flow.Session')
    @patch('src.pipeline.retrain_flow.get_run_logger')
    def test_fetch_data_no_data(self, mock_logger_instance, mock_session, mock_engine, mock_getenv):
        mock_logger = mock_logger_instance.return_value
        mock_getenv.side_effect = ['ahmed', '123', 'host', 'db', 'table_name']

        mock_result = MagicMock()

        dummy_query_result = []
        mock_result.fetchall.return_value = dummy_query_result

        mock_session_instance = MagicMock()
        mock_session_instance.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_session_instance

        with pytest.raises(ValueError, match="No data fetched from the databse. The table might be empty."):
            fetch_data.fn()


        mock_engine.assert_called_once_with('postgresql+psycopg://ahmed:123@host/db')
