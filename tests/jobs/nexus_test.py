from unittest.mock import patch, MagicMock
from jobs.nexus import job

def test_job():
    with patch('jobs.nexus.logger', new=MagicMock()) as mock_logger, \
         patch('jobs.nexus.read_data_from_custom_data_connector', new=MagicMock()) as mock_read_data:

        mock_file_content = "file content"
        mock_read_data.return_value = mock_file_content

        job()

        mock_logger.info.assert_any_call("Initializing Nexus job")
        mock_logger.info.assert_any_call(mock_file_content)
        mock_read_data.assert_called_once_with('DATA_BIL54.csv')
        mock_read_data.assert_called()
