import os
import pysftp
import fnmatch
import base64
import logging
import warnings

import utils.config

# Supress warning about trusting all host keys - bad practice!
warnings.filterwarnings('ignore', '.*Failed to load HostKeys.*')


logger = logging.getLogger(__name__)


def get_key(base64key, name, path='.'):
    if not os.path.isfile(os.path.join(path, name)):
        key_data = base64.b64decode(base64key).decode('utf-8')

        with open(os.path.join(path, name), 'w') as key_file:
            key_file.write(key_data)

    return os.path.abspath(os.path.join(path, name))


def get_filelist_and_connection(server):
    try:
        # Trust all host keys - bad practice!
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        
        server = server.upper()
        key_path = get_key(getattr(utils.config, server + '_SSH_KEY_BASE64'), server)
        key_pass = getattr(utils.config, server + '_SSH_KEY_PASS')
        host = getattr(utils.config, server + '_SFTP_HOST')
        username = getattr(utils.config, server + '_SFTP_USER')
        remote_dir = getattr(utils.config, server + '_SFTP_REMOTE_DIR')

        sftp = pysftp.Connection(host=host, username=username, private_key=key_path, private_key_pass=key_pass, cnopts=cnopts)

        # filter away directorires and files without file extensions
        filelist = [f for f in sftp.listdir(remote_dir) if fnmatch.fnmatch(f, '*.*')]

        return filelist, sftp
    except Exception as e:
        logger.error(e)
        return None, None
