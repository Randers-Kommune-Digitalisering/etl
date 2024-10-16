from unittest.mock import patch, MagicMock
from datetime import datetime

from jobindsats.jobindsats_y35a02 import (
    get_jobindsats_sho,
    dynamic_period,
    convert_to_datetime,
    period_request
)


def test_get_jobindsats_sho():
    with patch('jobindsats.jobindsats_y35a02.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y35a02.post_data_to_custom_data_connector') as mock_post_data:

        mock_period_request.return_value = [{'Period': ['2024M01', '2024M02']}]
        mock_client_request.return_value = [{
            'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
            'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
        }]
        mock_post_data.return_value = True

        result = get_jobindsats_sho()

        assert result
        mock_logger.info.assert_any_call("Starting jobindsats Selvforsørgelses- og hjemrejseydelse samt overgangsydelse")
        mock_logger.info.assert_any_call("Successfully updated JobindsatsY35A02")
        mock_post_data.assert_called_once()
        mock_client_request.assert_called_once()


def test_get_jobindsats_sho_post_data_failure():
    with patch('jobindsats.jobindsats_y35a02.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y35a02.post_data_to_custom_data_connector') as mock_post_data:

        mock_period_request.return_value = [{'Period': ['2024M01', '2024M02']}]
        mock_client_request.return_value = [{
            'Variables': [{'Label': 'Periode'}, {'Label': 'Area'}],
            'Data': [['2024M01', 'Randers'], ['2024M02', 'Aarhus']]
        }]
        mock_post_data.return_value = False

        result = get_jobindsats_sho()

        assert not result
        mock_logger.info.assert_any_call("Starting jobindsats Selvforsørgelses- og hjemrejseydelse samt overgangsydelse")
        mock_logger.error.assert_not_called()
        mock_post_data.assert_called_once()
        mock_client_request.assert_called_once()


def test_get_jobindsats_sho_exception():
    with patch('jobindsats.jobindsats_y35a02.logger') as mock_logger, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_client.make_request') as mock_client_request, \
         patch('jobindsats.jobindsats_y35a02.jobindsats_period_client.make_request') as mock_period_request, \
         patch('jobindsats.jobindsats_y35a02.post_data_to_custom_data_connector') as mock_post_data:

        mock_period_request.side_effect = Exception("Test exception")

        result = get_jobindsats_sho()

        assert not result
        mock_logger.info.assert_any_call("Starting jobindsats Selvforsørgelses- og hjemrejseydelse samt overgangsydelse")
        mock_logger.error.assert_called_once_with('Error Test exception')
        mock_post_data.assert_not_called()
        mock_client_request.assert_not_called()


def test_dynamic_period():
    latest_period = '2024M03'
    expected_period = [
        '2023M01', '2023M02', '2023M03', '2023M04', '2023M05', '2023M06', '2023M07', '2023M08', '2023M09', '2023M10', '2023M11', '2023M12',
        '2024M01', '2024M02', '2024M03',
        '2022M01', '2022M02', '2022M03', '2022M04', '2022M05', '2022M06', '2022M07', '2022M08', '2022M09', '2022M10', '2022M11', '2022M12'
    ]
    result = dynamic_period(latest_period)
    assert result == expected_period


def test_convert_to_datetime():
    period_str = '2024M02'
    expected_date = datetime(2024, 2, 1)
    result = convert_to_datetime(period_str)
    assert result == expected_date
