from io import StringIO, BytesIO
import pandas as pd
import fnmatch
import codecs
import string
import random
import logging

from utils.stfp import SFTPClient
from utils.config import CUSTOMDATA_SFTP_HOST, CUSTOMDATA_SFTP_USER, CUSTOMDATA_SFTP_REMOTE_DIR, CUSTOMDATA_SFTP_PASSWORD

logger = logging.getLogger(__name__)


def id_generator(size=8, chars=string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))


def download_data(f):
    sftp_client = SFTPClient(CUSTOMDATA_SFTP_HOST, CUSTOMDATA_SFTP_USER, CUSTOMDATA_SFTP_PASSWORD)
    conn = sftp_client.get_connection()

    filelist = [f for f in conn.listdir(CUSTOMDATA_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Meta*.*')] if conn else None
    filelist.sort()

    mdbody = StringIO()
    mdhead = StringIO()

    for file in filelist:
        print(file)

        try:
            flo = BytesIO()
            conn.getfo(f"{CUSTOMDATA_SFTP_REMOTE_DIR}/{file}", flo)
            flo.seek(0)

            df = pd.read_csv(flo, sep=';')

            df = df.sort_values(by=['TekniskNavn'])
            df = df[['TekniskNavn', 'EgetNavn']]

            write_markdown(df, file, mdhead, mdbody)
        except Exception as e:
            logger.error(e)
    return mdbody, mdhead


def write_markdown(df, file, head, body):
    file = file.replace('.csv', '').replace('Meta_', '')

    hook = id_generator()
    head.write(f"- [{file}](#{hook})\n")

    body.write(f"## <a id=\"{hook}\"> {file}</a> <a id=\"top\"> :arrow_heading_up: </a>\n")
    body.write("<details>\n<summary>Vis tabel</summary>\n\n")
    df.to_markdown(buf=body, index=False)
    body.write("\n</details>\n\n")

    return body, head


def gen_documentation():
    f = codecs.open("../doc/Data_i_Custom_Data.md", "w", "utf-8")
    f.write("## <a id=\"top\">Data i Custom Data i KMD Insight</a>\n")

    body, head = download_data(f)
    print(head.getvalue(), file=f)
    print(body.getvalue(), file=f)

    f.close()


gen_documentation()
