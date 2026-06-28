import pytest
from unittest.mock import MagicMock, patch
from src.pipeline.drift_check import read_data_from_db
class Test_readDB:

    @patch('src.pipeline.drift_check.get_run_logger')
    @patch('src.pipeline.drift_check.create_engine')
    @patch('src.pipeline.drift_check.Session')
    @patch('os.getenv')
    def test_read_from_db(self, mock_getenv, mock_session, mock_engine, mock_get_run_logger):
        mock_getenv.side_effect = ['ahmed', '123', 'host', 'db', 'table_name']

        mock_result = MagicMock() # mock the result of session.execute

        # because the code query from db 48 rows we create 48 dummy query
        dummy_query_result = [(i, f"2026-01-22 10:00{i%60:02d}", i*2) for i in range(48)]
        mock_result.fetchall.return_value = dummy_query_result
        mock_result.keys.return_value = ['id', 'name', 'updated']

        mock_session_instance = MagicMock()
        mock_session_instance.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_session_instance

        current_data, refrence_data = read_data_from_db.fn()

        assert len(current_data) == 24
        assert len(refrence_data) == 24
        assert list(current_data.columns) == ['id', 'name', 'updated']

        mock_engine.assert_called_once_with('postgresql+psycopg://ahmed:123@host/db')

        mock_logger = mock_get_run_logger.return_value
        mock_logger.info.assert_any_call("Data fetched successfully from the database.")

    @patch('src.pipeline.drift_check.get_run_logger')
    @patch('src.pipeline.drift_check.create_engine')
    @patch('src.pipeline.drift_check.Session')
    @patch('os.getenv')
    def test_read_from_db_insufficient_data(self, mock_getenv, mock_session, mock_engine, mock_get_run_logger):
        mock_getenv.side_effect = ['ahmed', '123', 'host', 'db', 'table_name']

        mock_result = MagicMock()

        dummy_query_result = [(i, f"2026-01-22 10:00{i%60:02d}", i*2) for i in range(30)]
        mock_result.fetchall.return_value = dummy_query_result
        mock_result.keys.return_value = ['id', 'name', 'updated']

        mock_session_instance = MagicMock()
        mock_session_instance.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_session_instance

        mock_logger = mock_get_run_logger.return_value

        with pytest.raises(ValueError, match="Insufficient data to split"):
            read_data_from_db.fn()
        mock_logger.warning.assert_called_once()

    @patch('src.pipeline.drift_check.get_run_logger')
    @patch('src.pipeline.drift_check.create_engine')
    @patch('src.pipeline.drift_check.Session')
    @patch('os.getenv')
    def test_read_from_db_missing_column(self, mock_getenv, mock_session, mock_engine, mock_get_run_logger):
        mock_getenv.side_effect = ['ahmed', '123', 'host', 'db', 'table_name']

        mock_result = MagicMock()

        dummy_query_result = []
        mock_result.fetchall.return_value = dummy_query_result
        mock_result.keys.return_value = ['id', 'name']

        mock_session_instance = MagicMock()
        mock_session_instance.execute.return_value = mock_result
        mock_session.return_value.__enter__.return_value = mock_session_instance

        mock_logger = mock_get_run_logger.return_value

        with pytest.raises(KeyError):
            read_data_from_db.fn()

        assert mock_logger.error.called
