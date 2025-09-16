import io
import logging
import pycurl

# Default port for FTPS is 990

logger = logging.getLogger()


class FTPSClient:
    def __init__(self, host: str, username: str, password: str):
        if not host.startswith('ftps://'):
            host = f'ftps://{host}'
        self.host = host
        self.username = username
        self.password = password

    def listdir(self, path: str = '.'):
        if path == '.':
            host = self.host
        else:
            host = f"{self.host}/{path.lstrip('/')}"

        buffer = io.BytesIO()

        c = pycurl.Curl()
        c.setopt(c.URL, host)
        c.setopt(c.USERPWD, f'{self.username}:{self.password}')
        c.setopt(c.SSL_VERIFYPEER, 0)
        c.setopt(c.SSL_VERIFYHOST, 0)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.FTP_SSL, pycurl.FTPSSL_ALL)
        c.setopt(c.FTPSSLAUTH, pycurl.FTPAUTH_TLS)
        c.setopt(c.FTP_USE_EPSV, 1)
        c.setopt(c.DIRLISTONLY, False)
        c.perform()
        c.close()

        listing = buffer.getvalue().decode('utf-8')
        return parse_list_output(listing)

    def put(self, filename: str, filedata: io.BytesIO):
        try:
            if '.' not in filename:
                raise ValueError('No file extenstion in filename')

            filedata.seek(0)
            host = f"{self.host}/{filename.strip('/')}"

            upload_curl = pycurl.Curl()
            upload_curl.setopt(upload_curl.URL, f"{host}/{filename}")
            upload_curl.setopt(upload_curl.USERPWD, f'{self.username}:{self.password}')
            upload_curl.setopt(upload_curl.SSL_VERIFYPEER, 0)
            upload_curl.setopt(upload_curl.SSL_VERIFYHOST, 0)
            upload_curl.setopt(upload_curl.UPLOAD, 1)
            upload_curl.setopt(upload_curl.READDATA, filedata)
            upload_curl.setopt(upload_curl.FTP_SSL, pycurl.FTPSSL_ALL)
            upload_curl.setopt(upload_curl.FTPSSLAUTH, pycurl.FTPAUTH_TLS)
            upload_curl.setopt(upload_curl.FTP_USE_EPSV, 1)
            upload_curl.perform()
            upload_curl.close()

            return True
        except Exception as e:
            logger.error(e)
            return False


# Helper functions
def parse_list_output(ls_output: str):
    lines = ls_output.strip().split('\n')
    entries = []

    for line in lines:
        parts = line.split()
        if len(parts) >= 9:
            name = ' '.join(parts[8:])
            entries.append(name)
    return entries
