import pytest
from unittest.mock import patch

from jobindsats.jobindsats_otij01 import (
    get_jobindsats_ydelsesmodtagere_loentimer
)


@pytest.fixture
def mock_dependencies():
    with patch('jobindsats.jobindsats_otij01.logger') as mock_logger, \
         patch('jobindsats.jobindsats_otij01.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_otij01.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_otij01.post_data_to_custom_data_connector') as mock_post_data:
        yield {
            'mock_logger': mock_logger,
            'mock_client_request': mock_client_request,
            'mock_period_request': mock_period_request,
            'mock_post_data': mock_post_data
        }


def test_get_jobindsats_ydelsesmodtagere_loentimer(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = True

    result = get_jobindsats_ydelsesmodtagere_loentimer()

    assert result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Ydelsesmodtagere med løntimer")
    mock_dependencies['mock_logger'].info.assert_any_call("Successfully updated JobindsatsOTIJ01")
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_ydelsesmodtagere_loentimer_post_data_failure(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = False

    result = get_jobindsats_ydelsesmodtagere_loentimer()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Ydelsesmodtagere med løntimer")
    mock_dependencies['mock_logger'].error.assert_not_called()
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_ydelsesmodtagere_loentimer_exception(mock_dependencies):
    mock_dependencies['mock_period_request'].side_effect = Exception("Test exception")

    result = get_jobindsats_ydelsesmodtagere_loentimer()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Ydelsesmodtagere med løntimer")
    mock_dependencies['mock_logger'].error.assert_called_once_with('Error Test exception')
    mock_dependencies['mock_post_data'].assert_not_called()
    mock_dependencies['mock_client_request'].assert_not_called()
