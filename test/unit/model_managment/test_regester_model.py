import pytest
from unittest.mock import patch, MagicMock


from src.model_managment.regester_model import compare_models, register_best_model

class Test_register:
    @patch("src.model_managment.regester_model.client")
    @patch("src.model_managment.regester_model.get_experment")
    @patch("src.model_managment.regester_model.compare_models")
    @patch("mlflow.register_model")
    @patch("os.getenv")
    def test_register_best_model_happy_path(self, mock_getenv, mock_register, mock_compare, mock_get_exp, mock_client):
        mock_getenv.return_value = "my_awesome_model"

        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "exp_123"
        mock_get_exp.return_value = mock_experiment

        mock_run = MagicMock()
        mock_run.info.run_id = "best_run_999"

        mock_client.search_runs.return_value = [mock_run]
        mock_compare.return_value = True

        mock_register_result = MagicMock()
        mock_register_result.version = "1"
        mock_register.return_value = mock_register_result


        register_best_model()


        mock_register.assert_called_once_with(
            model_uri="runs:/best_run_999/model",
            name="my_awesome_model"
        )

        mock_client.set_registered_model_alias.assert_called_once_with(
            name="my_awesome_model",
            alias="production",
            version="1"
        )

    @patch("src.model_managment.regester_model.client")
    @patch("src.model_managment.regester_model.get_experment")
    def test_register_best_model_no_experiment(self, mock_get_exp, mock_client):
        mock_get_exp.return_value = None

        register_best_model()

        mock_client.search_runs.assert_not_called()


    @patch("src.model_managment.regester_model.client")
    @patch("src.model_managment.regester_model.get_experment")
    @patch("src.model_managment.regester_model.compare_models")
    def test_register_best_model_no_runs_exist(self, mock_compare, mock_get_exp, mock_client):
        mock_experiment = MagicMock()
        mock_experiment.experiment_id = "exp_123"
        mock_get_exp.return_value = mock_experiment

        mock_client.search_runs.return_value = []

        register_best_model()

        mock_compare.assert_not_called()


class Test_compare:

    @patch("src.model_managment.regester_model.archive_old_model")
    @patch("src.model_managment.regester_model.predict")
    @patch("mlflow.pyfunc.load_model")
    @patch("os.getenv")
    def test_compare_models_new_is_better(self, mock_getenv, mock_load_model, mock_predict, mock_archive):
        """Test scenario where the new model outperforms the old one."""
        mock_getenv.return_value = "my_registered_model"

        mock_load_model.return_value = MagicMock()

        mock_predict.side_effect = [0.2, 0.5]

        mock_archive.return_value = True

        result = compare_models("fake_run_id_123")

        assert result is True
        assert mock_load_model.call_count == 2
        assert mock_predict.call_count == 2
        mock_archive.assert_called_once()


    @patch("src.model_managment.regester_model.predict")
    @patch("src.model_managment.regester_model.mlflow.pyfunc.load_model")
    def test_compare_models_old_is_better(self, mock_load_model, mock_predict):
        """Test scenario where the old model is still better."""
        mock_load_model.return_value = MagicMock()

        # new_model_result (0.8) > old_model_result (0.5), so old is better
        mock_predict.side_effect = [0.8, 0.5]

        result = compare_models("fake_run_id_123")

        assert result is False


    @patch("src.model_managment.regester_model.predict")
    @patch("src.model_managment.regester_model.mlflow.pyfunc.load_model")
    def test_compare_models_no_old_model_exists(self, mock_load_model, mock_predict):
        """Test the safety net when there is no old model (first time run)."""
        mock_load_model.side_effect = [MagicMock(), Exception("Model not found")]

        mock_predict.return_value = 0.5

        result = compare_models("fake_run_id_123")

        assert result is True
        assert mock_predict.call_count == 1
