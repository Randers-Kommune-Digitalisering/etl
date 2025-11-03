import logging

from utils.api_requests import APIClient
from utils.config import MAIL_SERVER_URL

logger = logging.getLogger(__name__)
mail_client = APIClient(base_url=MAIL_SERVER_URL)


def send_mail_with_attachment(to_mail, from_mail, title, body, file_name, file_bytes):
    payload = {
        'from': from_mail,
        'to': to_mail,
        'title': title,
        'body': body,
        'attachments': {'filename': file_name, 'content': list(file_bytes.getvalue())}
    }

    if mail_client.make_request(method='POST', json=payload):
        logger.info(f'Successfully sent email with attachment to {to_mail}')
    else:
        logger.error(f'Failed to send email with attachment to {to_mail}')


def send_mail(to_mail, from_mail, title, body):
    payload = {
        'from': from_mail,
        'to': to_mail,
        'title': title,
        'body': body
    }

    if mail_client.make_request(method='POST', json=payload):
        logger.info(f'Successfully sent email to {to_mail}')
    else:
        logger.error(f'Failed to send email to {to_mail}')
