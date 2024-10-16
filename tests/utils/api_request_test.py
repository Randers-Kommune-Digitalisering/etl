import pytest
import base64
from unittest.mock import MagicMock, patch
from utils.api_requests import APIClient

@pytest.fixture
def api_client():
    return APIClient('http://testurl.com')

@pytest.fixture
def cert_base64():
    return base64.b64encode(b'test_cert_data').decode('utf-8')

def test_init(api_client):
    assert api_client.base_url == 'http://testurl.com'

def test_cert_base64(cert_base64):
    api_client = APIClient('http://testurl.com', cert_base64=cert_base64)
    assert api_client.cert_data == b'test_cert_data'

def test_no_cert_base64(api_client):
    assert api_client.cert_data is None


@patch('requests_pkcs12.post')
def test_make_request_get_cert(mock_get):
    test_base64 = base64.b64encode(b'test_cert')
    api_client = APIClient('http://testurl.com', cert_base64=test_base64, password='test_pass')

    res = MagicMock()
    res.raise_for_status.return_value = None
    res.content = b'ok'
    mock_get.return_value = res

    assert api_client.make_request(path='/test', json={'test': 'test'}) == b'ok'
    mock_get.assert_called_once_with('http://testurl.com/test', json={'test': 'test'}, pkcs12_data=b'test_cert', pkcs12_password='test_pass', headers={'Content-Type': 'application/json'})

@patch('requests.get')
def test_make_request_get(mock_get):
    api_client = APIClient('http://testurl.com', api_key='test_key')

    res = MagicMock()
    res.raise_for_status.return_value = None
    res.headers.get.return_value = 'application/json'
    res.json.return_value = {'test': 'test'}
    mock_get.return_value = res

    assert api_client.make_request(path='/test') == {'test': 'test'}
    mock_get.assert_called_once_with('http://testurl.com/test', headers={'Authorization': 'test_key'})

@patch('requests.post')
def test_make_request_post(mock_get):
    api_client = APIClient('http://testurl.com', api_key='Bearer test_key')

    res = MagicMock()
    res.raise_for_status.return_value = None
    res.content = b''
    mock_get.return_value = res

    assert api_client.make_request(path='/test', json={'test': 'test'}) == b' '
    mock_get.assert_called_once_with('http://testurl.com/test', json={'test': 'test'}, headers={'Authorization': 'Bearer test_key', 'Content-Type': 'application/json'})

@patch('requests.put')
def test_make_request_put(mock_put):
    api_client = APIClient('http://testurl.com', api_key='Bearer test_key')

    res = MagicMock()
    res.raise_for_status.return_value = None
    res.content = b'ok'
    mock_put.return_value = res

    assert api_client.make_request(method='put', path='/', data='test', headers={'custom': 'header'}) == b'ok'
    mock_put.assert_called_once_with('http://testurl.com/', data='test', headers={'custom': 'header', 'Authorization': 'Bearer test_key'})

@patch('requests.delete')
def test_make_request_delete(mock_delete):
    api_client = APIClient('http://testurl.com', api_key='Bearer test_key')
    
    res = MagicMock()
    res.raise_for_status.return_value = None
    res.content = b'ok'
    mock_delete.return_value = res

    assert api_client.make_request(method='delete', path='/test') == b'ok'
    mock_delete.assert_called_once_with('http://testurl.com/test', headers={'Authorization': 'Bearer test_key'})

