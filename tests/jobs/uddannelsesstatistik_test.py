import pytest
from unittest.mock import patch

from jobs.uddannelsesstatistik import (
    get_well_being_data
)


@pytest.fixture
def mock_dependencies():
    with patch('jobs.uddannelsesstatistik.logger') as mock_logger, \
         patch('jobs.uddannelsesstatistik.udddannels_stattistik_client.make_request') as mock_client_request, \
         patch('jobs.uddannelsesstatistik.post_data_to_custom_data_connector') as mock_post_data:
        yield {
            'mock_logger': mock_logger,
            'mock_client_request': mock_client_request,
            'mock_post_data': mock_post_data
        }


def test_get_well_being_data_success(mock_dependencies):
    mock_dependencies['mock_client_request'].return_value = {
        'Randers Real Skolen': {
            'Udskoling': {
                '2021/2022': {
                    'Trivselsindikator 2': {
                        'Value': 10
                    }
                }
            }
        }
    }
    mock_dependencies['mock_post_data'].return_value = True

    result = get_well_being_data()
    assert result
    mock_dependencies['mock_logger'].info.assert_called_with('Updated "Elevtrivsel 4. til 9. klasse"')
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_well_being_data_failure(mock_dependencies):
    mock_dependencies['mock_client_request'].return_value = {
        'Randers Real Skolen': {
            'Udskoling': {
                '2021/2022': {
                    'Trivselsindikator 2': {
                        'Value': 10
                    }
                }
            }
        }
    }
    mock_dependencies['mock_post_data'].return_value = False

    result = get_well_being_data()

    assert not result
    mock_dependencies['mock_logger'].error.assert_called_with('Failed to update "Elevtrivsel 4. til 9. klasse"')
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_well_being_exception(mock_dependencies):
    mock_dependencies['mock_client_request'].side_effect = Exception("Test exception")

    result = get_well_being_data()

    assert not result
    mock_dependencies['mock_logger'].error.assert_called_once()
    logged_error = mock_dependencies['mock_logger'].error.call_args[0][0]
    assert str(logged_error) == "Test exception"
    mock_dependencies['mock_post_data'].assert_not_called()
    mock_dependencies['mock_client_request'].assert_called_once()
