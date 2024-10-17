import pytest
from unittest.mock import patch, MagicMock
from jobs.jobindsats import job

@pytest.fixture
def mock_dependencies():
    with patch('jobs.jobindsats.logger', new=MagicMock()) as mock_logger, \
         patch('jobs.jobindsats.get_jobindsats_alle_ydelser', new=MagicMock()) as mock_alle_ydelser, \
         patch('jobs.jobindsats.get_jobindsats_tilbud_samtaler', new=MagicMock()) as mock_tilbud_samtaler, \
         patch('jobs.jobindsats.get_jobindsats_ydelsesmodtagere_loentimer', new=MagicMock()) as mock_ydelsesmodtagere_loentimer, \
         patch('jobs.jobindsats.get_jobindsats_tilbagetraekningsydelser', new=MagicMock()) as mock_tilbagetraekningsydelser, \
         patch('jobs.jobindsats.get_jobindsats_ydelse_til_job', new=MagicMock()) as mock_ydelse_til_job, \
         patch('jobs.jobindsats.get_jobindsats_revalidering', new=MagicMock()) as mock_revalidering, \
         patch('jobs.jobindsats.get_jobindsats_ressourceforløb', new=MagicMock()) as mock_ressourceforløb, \
         patch('jobs.jobindsats.get_jobindsats_uddannelseshjælp', new=MagicMock()) as mock_uddannelseshjælp, \
         patch('jobs.jobindsats.get_jobindsats_kontanthjælp', new=MagicMock()) as mock_kontanthjælp, \
         patch('jobs.jobindsats.get_jobindsats_sho', new=MagicMock()) as mock_sho, \
         patch('jobs.jobindsats.get_jobindsats_jobafklaringsforløb', new=MagicMock()) as mock_jobafklaringsforløb, \
         patch('jobs.jobindsats.get_jobindsats_ledighedsydelse', new=MagicMock()) as mock_ledighedsydelse, \
         patch('jobs.jobindsats.get_jobindsats_fleksjob', new=MagicMock()) as mock_fleksjob, \
         patch('jobs.jobindsats.get_jobindsats_syg_dagpenge', new=MagicMock()) as mock_syg_dagpenge, \
         patch('jobs.jobindsats.get_jobindsats_dagpenge', new=MagicMock()) as mock_dagpenge, \
         patch('jobs.jobindsats.get_jobindats_ydelsesgrupper', new=MagicMock()) as mock_ydelsesgrupper:
        yield {
            'mock_logger': mock_logger,
            'mock_alle_ydelser': mock_alle_ydelser,
            'mock_tilbud_samtaler': mock_tilbud_samtaler,
            'mock_ydelsesmodtagere_loentimer': mock_ydelsesmodtagere_loentimer,
            'mock_tilbagetraekningsydelser': mock_tilbagetraekningsydelser,
            'mock_ydelse_til_job': mock_ydelse_til_job,
            'mock_revalidering': mock_revalidering,
            'mock_ressourceforløb': mock_ressourceforløb,
            'mock_uddannelseshjælp': mock_uddannelseshjælp,
            'mock_kontanthjælp': mock_kontanthjælp,
            'mock_sho': mock_sho,
            'mock_jobafklaringsforløb': mock_jobafklaringsforløb,
            'mock_ledighedsydelse': mock_ledighedsydelse,
            'mock_fleksjob': mock_fleksjob,
            'mock_syg_dagpenge': mock_syg_dagpenge,
            'mock_dagpenge': mock_dagpenge,
            'mock_ydelsesgrupper': mock_ydelsesgrupper
        }


def test_job_success(mock_dependencies):
    result = job()

    assert result
    mock_dependencies['mock_logger'].info.assert_any_call('Starting jobindsats ETL jobs!')
    mock_dependencies['mock_alle_ydelser'].assert_called_once()
    mock_dependencies['mock_tilbud_samtaler'].assert_called_once()
    mock_dependencies['mock_ydelsesmodtagere_loentimer'].assert_called_once()
    mock_dependencies['mock_tilbagetraekningsydelser'].assert_called_once()
    mock_dependencies['mock_ydelse_til_job'].assert_called_once()
    mock_dependencies['mock_revalidering'].assert_called_once()
    mock_dependencies['mock_ressourceforløb'].assert_called_once()
    mock_dependencies['mock_uddannelseshjælp'].assert_called_once()
    mock_dependencies['mock_kontanthjælp'].assert_called_once()
    mock_dependencies['mock_sho'].assert_called_once()
    mock_dependencies['mock_jobafklaringsforløb'].assert_called_once()
    mock_dependencies['mock_ledighedsydelse'].assert_called_once()
    mock_dependencies['mock_fleksjob'].assert_called_once()
    mock_dependencies['mock_syg_dagpenge'].assert_called_once()
    mock_dependencies['mock_dagpenge'].assert_called_once()
    mock_dependencies['mock_ydelsesgrupper'].assert_called_once()


def test_job_exception(mock_dependencies):
    mock_dependencies['mock_alle_ydelser'].side_effect = Exception('Test exception')

    result = job()

    assert not result
    mock_dependencies['mock_logger'].info.assert_called_once_with('Starting jobindsats ETL jobs!')
    mock_dependencies['mock_logger'].error.assert_called_once_with('An error occurred: Test exception')
