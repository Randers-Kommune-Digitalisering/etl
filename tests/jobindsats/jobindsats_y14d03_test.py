import pytest
from unittest.mock import patch
from datetime import datetime

from jobindsats.jobindsats_y14d03 import (
    get_jobindsats_ydelse_til_job,
    dynamic_period,
    convert_to_datetime,
    period_request
)


@pytest.fixture
def mock_dependencies():
    with patch('jobindsats.jobindsats_y14d03.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y14d03.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y14d03.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y14d03.post_data_to_custom_data_connector') as mock_post_data:
        yield {
            'mock_logger': mock_logger,
            'mock_client_request': mock_client_request,
            'mock_period_request': mock_period_request,
            'mock_post_data': mock_post_data
        }


def test_get_jobindsats_ydelse_til_job(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2022QMAT01', '2022QMAT02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2022QMAT01', 'Randers'], ['2022QMAT02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = True

    result = get_jobindsats_ydelse_til_job()

    assert result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Fra ydelse til job")
    mock_dependencies['mock_logger'].info.assert_any_call("Successfully updated JobindsatsY14D03")
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_ydelse_til_job_post_data_failure(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2022QMAT01', '2022QMAT02']}]
    mock_dependencies['mock_client_request'].return_value = [{
        'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
        'Data': [['2022QMAT01', 'Randers'], ['2022QMAT02', 'Aarhus']]
    }]
    mock_dependencies['mock_post_data'].return_value = False

    result = get_jobindsats_ydelse_til_job()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Fra ydelse til job")
    mock_dependencies['mock_logger'].error.assert_not_called()
    mock_dependencies['mock_post_data'].assert_called_once()
    mock_dependencies['mock_client_request'].assert_called_once()


def test_get_jobindsats_ydelse_til_job_exception(mock_dependencies):
    mock_dependencies['mock_period_request'].side_effect = Exception("Test exception")

    result = get_jobindsats_ydelse_til_job()

    assert not result
    mock_dependencies['mock_logger'].info.assert_any_call("Starting jobindsats Fra ydelse til job")
    mock_dependencies['mock_logger'].error.assert_called_once_with('Error Test exception')
    mock_dependencies['mock_post_data'].assert_not_called()
    mock_dependencies['mock_client_request'].assert_not_called()


def test_dynamic_period():
    latest_period = "2024QMAT04"
    expected_period = [
        '2023QMAT01', '2023QMAT02', '2023QMAT03', '2023QMAT04', '2023QMAT05', '2023QMAT06',
        '2023QMAT07', '2023QMAT08', '2023QMAT09', '2023QMAT10', '2023QMAT11', '2023QMAT12',
        '2024QMAT01', '2024QMAT02', '2024QMAT03', '2024QMAT04',
        '2022QMAT01', '2022QMAT02', '2022QMAT03', '2022QMAT04', '2022QMAT05', '2022QMAT06',
        '2022QMAT07', '2022QMAT08', '2022QMAT09', '2022QMAT10', '2022QMAT11', '2022QMAT12'
    ]
    result = dynamic_period(latest_period)
    result == expected_period


def test_convert_to_datetime():
    period_str = "2022QMAT02"
    expected_date = datetime(2022, 4, 1)
    result = convert_to_datetime(period_str)
    result == expected_date


def test_period_request(mock_dependencies):
    mock_dependencies['mock_period_request'].return_value = [{'Period': ['2022QMAT02', '2022QMAT04']}]
    latest_period = period_request()
    assert latest_period == '2022QMAT04'
