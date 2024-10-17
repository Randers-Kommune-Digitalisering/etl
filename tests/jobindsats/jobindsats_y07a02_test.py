import pytest
from unittest.mock import patch
from datetime import datetime

from jobindsats.jobindsats_y07a02 import (
    get_jobindsats_syg_dagpenge,
    dynamic_period,
    convert_to_datetime,
    period_request
)


@pytest.fixture
def mock_dependencies():
    with patch('jobindsats.jobindsats_y07a02.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y07a02.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y07a02.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y07a02.post_data_to_custom_data_connector') as mock_post_data:
        yield {
            'mock_logger': mock_logger,
            'mock_client_request': mock_client_request,
            'mock_period_request': mock_period_request,
            'mock_post_data': mock_post_data
        }


def test_get_jobindsats_syg_dagpenge(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = True

    result = get_jobindsats_syg_dagpenge()

    assert result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Sygedagppenge")
    mock_dependencies['mock_logger'].info.assert_any_call("Successfully updated JobindsatsY07A02")
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_syg_dagpenge_post_data_failure(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = False

    result = get_jobindsats_syg_dagpenge()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Sygedagppenge")
    mock_dependencies['mock_logger'].error.assert_not_called()
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_syg_dagpenge_exception(mock_dependencies):
    mock_dependencies['mock_period_request'].side_effect = Exception("Test exception")

    result = get_jobindsats_syg_dagpenge()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Sygedagppenge")
    mock_dependencies['mock_logger'].error.assert_called_once_with('Error Test exception')
    mock_dependencies['mock_post_data'].assert_not_called()
    mock_dependencies['mock_client_request'].assert_not_called()


def test_dynamic_period():
    latest_period = '2024M02'
    expected_period = [
        '2023M01', '2023M02', '2023M03', '2023M04', '2023M05', '2023M06', '2023M07', '2023M08', '2023M09', '2023M10', '2023M11', '2023M12',
        '2024M01', '2024M02'
    ]
    result = dynamic_period(latest_period)
    assert result == expected_period


def test_convert_to_datetime():
    period_str = '2023M012'
    expected_date = datetime(2023, 12, 1)
    result = convert_to_datetime(period_str)
    assert result == expected_date


def test_period_request(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2024M01', '2024M02']}]
    result = period_request()

    assert result == '2024M02'
    mock_dependencies['mock_period_request'].assert_called_once()
