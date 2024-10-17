import pytest
from unittest.mock import patch
from datetime import datetime

from jobindsats.jobindsats_y12a02 import (
    get_jobindsats_jobafklaringsforløb
)


@pytest.fixture
def mock_dependencies():
    with patch('jobindsats.jobindsats_y12a02.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y12a02.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y12a02.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y12a02.post_data_to_custom_data_connector') as mock_post_data:
        yield {
            'mock_logger': mock_logger,
            'mock_client_request': mock_client_request,
            'mock_period_request': mock_period_request,
            'mock_post_data': mock_post_data
        }


def test_get_jobindsats_jobafklaringsforløb(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = True

    result = get_jobindsats_jobafklaringsforløb()

    assert result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Jobafklaringsforløb")
    mock_dependencies['mock_logger'].info.assert_any_call("Successfully updated JobindsatsY12A02")
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_jobafklaringsforløb_post_data_failure(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = False

    result = get_jobindsats_jobafklaringsforløb()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Jobafklaringsforløb")
    mock_dependencies['mock_logger'].error.assert_not_called()
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_jobafklaringsforløb_exception(mock_dependencies):
    mock_dependencies['mock_period_request'].side_effect = Exception("Test exception")

    result = get_jobindsats_jobafklaringsforløb()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Jobafklaringsforløb")
    mock_dependencies['mock_logger'].error.assert_called_once_with('Error Test exception')
    mock_dependencies['mock_post_data'].assert_not_called()
    mock_dependencies['mock_client_request'].assert_not_called()
