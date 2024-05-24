import os
import pysftp
import fnmatch
import base64
import logging

import utils.config
# Supress warning about trusting all host keys - bad practice!
import warnings
warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')


logger = logging.getLogger(__name__)


def get_key(base64key, name, path='.'):
    if not os.path.isfile(os.path.join(path, name)):
        key_data = base64.b64decode(base64key).decode('utf-8')

        with open(os.path.join(path, name), 'w') as key_file:
            key_file.write(key_data)

    return os.path.abspath(os.path.join(path, name))


def get_filelist_and_connection(server):
    # Trust all host keys - bad practice!
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None

    server = server.upper()
    key_path = get_key(server + '_SSH_KEY_BASE64', server)
    host = getattr(utils.config, server + '_SFTP_HOST')
    username = getattr(utils.config, server + '_SFTP_USER')

    # Get connection
    sftp = pysftp.Connection(host=host, username=username, private_key=key_path, cnopts=cnopts)

    # filter away directorires and files without file extensions
    filelist = [f for f in sftp.listdir() if fnmatch.fnmatch(f, '*.*')]

    return filelist, sftp
