"""Copies riksprot metadata CSV to PostgreSQL database"""

import tempfile
from glob import glob
from os.path import basename, join, splitext

import dotenv
import pandas as pd
from loguru import logger
from sqlalchemy import Engine, create_engine

from pyriksprot.metadata import gh_dl_metadata_extra
from pyriksprot.metadata.utility import fix_incomplete_datetime_series


def csv2pgsql(tag: str):
    """Download CSV for version to temporary folder"""
    dotenv.load_dotenv(override=True)

    with tempfile.TemporaryDirectory() as folder:
        logger.info('using temporary directory', folder)
        gh_dl_metadata_extra(folder=folder, tag=tag)

        data = load_data_frames(join(folder, tag, '*.csv'), sep=',')

        fix_incomplete_datetime_series(data['person'], 'born', action='truncate')
        data['government']['end'] = pd.to_datetime(data['government']['end'])

        db: Engine = create_engine('postgresql://humlab_admin:Vua9VagZ@humlabseadserv.srv.its.umu.se/westac')
        store_data_frames(data, db)


def store_data_frames(data: dict[str, pd.DataFrame], db: Engine) -> None:
    with db.connect() as con:
        for tablename in data:
            data[tablename].to_sql(tablename, con, if_exists='replace', index=False)
        con.commit()


def load_data_frames(pattern: str = '*.csv', sep: str = ',') -> dict[str, pd.DataFrame]:
    data: dict = {}
    for path in glob(pattern):
        tablename: str = splitext(basename(path))[0]
        data[tablename] = pd.read_csv(path, sep=sep)
    return data


def doit():
    tag: str = "v0.6.0"
    csv2pgsql(tag)
