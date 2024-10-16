from utils.stfp import SFTPClient
from unittest.mock import patch, MagicMock


@patch('pysftp.Connection')
def test_get_connection_success(mock_connection):
    client = SFTPClient('host', 'user', 'pass')
    mock_connection.return_value = MagicMock()

    result = client.get_connection()

    assert result is not None
    mock_connection.assert_called_once_with(host='host', username='user', password='pass', private_key=None, cnopts=client.cnopts)
