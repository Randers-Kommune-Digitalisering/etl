from unittest.mock import patch, MagicMock
from jobs.jobindsats import job


def test_job_success():
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

        result = job()

        assert result
        mock_logger.info.assert_called_once_with('Starting jobindsats ETL jobs!')
        mock_alle_ydelser.assert_called_once()
        mock_tilbud_samtaler.assert_called_once()
        mock_ydelsesmodtagere_loentimer.assert_called_once()
        mock_tilbagetraekningsydelser.assert_called_once()
        mock_ydelse_til_job.assert_called_once()
        mock_revalidering.assert_called_once()
        mock_ressourceforløb.assert_called_once()
        mock_uddannelseshjælp.assert_called_once()
        mock_kontanthjælp.assert_called_once()
        mock_sho.assert_called_once()
        mock_jobafklaringsforløb.assert_called_once()
        mock_ledighedsydelse.assert_called_once()
        mock_fleksjob.assert_called_once()
        mock_syg_dagpenge.assert_called_once()
        mock_dagpenge.assert_called_once()
        mock_ydelsesgrupper.assert_called_once()


def test_job_exception():
    with patch('jobs.jobindsats.logger', new=MagicMock()) as mock_logger, \
         patch('jobs.jobindsats.get_jobindsats_alle_ydelser', new=MagicMock(side_effect=Exception('Test exception'))):

        result = job()

        assert not result
        mock_logger.info.assert_called_once_with('Starting jobindsats ETL jobs!')
        mock_logger.error.assert_called_once_with('An error occurred: Test exception')
