import os
import pathlib
import shutil
import sqlite3
from contextlib import closing
import uuid

import pandas as pd
import pytest

import pyriksprot.sql as sql
from pyriksprot import metadata as md
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus.parlaclarin import ProtocolMapper

jj = os.path.join

DUMMY_METADATA_DATABASE_NAME: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'


def test_list_sql_files():
    tag: str = ConfigStore.config().get("metadata.version")
    data = sql.sql_file_paths(tag)
    assert len(data) > 0


def test_gh_ls():
    tag: str = ConfigStore.config().get("metadata.version")

    data: list[dict] = md.gh_ls("welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", tag)
    assert len(data) > 0


def test_download_metadata(tmp_path: pathlib.Path):

    tag: str = ConfigStore.config().get("metadata.version")

    data: list[dict] = md.gh_ls("welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", tag)
    assert len(data) > 0

    filenames: list[str] = md.gh_dl_metadata_extra(folder=tmp_path, tag=tag, force=True)
    assert len(filenames) > 0

    shutil.rmtree(str(tmp_path))


def test_get_and_set_db_version():
    dummy_db_name: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'

    service: md.DatabaseHelper = md.DatabaseHelper(dummy_db_name)

    tag: str = "kurt"
    service.set_tag(tag=tag)
    stored_tag: str = service.get_tag()
    assert tag == stored_tag
    service.verify_tag(tag=tag)

    tag: str = "olle"
    service.set_tag(tag=tag)
    stored_tag: str = service.get_tag()
    assert tag == stored_tag
    assert tag == stored_tag


def test_create_metadata_database():
    tag: str = ConfigStore.config().get("metadata.version")
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
    service: md.DatabaseHelper = md.DatabaseHelper(target_filename)
    service.create(tag=tag, folder=source_folder, force=True)

    assert os.path.isfile(target_filename)

    service.verify_tag(tag=tag)

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.DatabaseHelper(target_filename).create(tag=None, folder=source_folder, force=True)


@pytest.mark.parametrize(
    'corpus_folder',
    [
        ConfigStore.config().get("corpus.folder"),
        ConfigStore.config().get("fakes.folder"),
    ],
)
def test_generate_corpus_indexes(corpus_folder: str):
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    data: dict[str, pd.DataFrame] = factory.generate(corpus_folder=corpus_folder, target_folder="tests/output").data

    assert data.get('protocols') is not None
    assert data.get('utterances') is not None
    assert data.get('speaker_notes') is not None


def _db_table_exists(database_filename: str, table: str):
    with closing(sqlite3.connect(database_filename)) as db:
        with closing(db.cursor()) as cursor:
            cursor.execute(f"select count(name) from sqlite_master where type='table' and name='{table}'")
            return cursor.fetchone()[0] == 1


def test_generate_and_load_corpus_indexes():
    version: str = ConfigStore.config().get("metadata.version")
    corpus_folder: str = jj("./tests/test_data/source", version, "parlaclarin")
    target_folder: str = f"./tests/output/{str(uuid.uuid4())[:8]}"
    database_filename: str = f'./tests/output/{str(uuid.uuid4())[:8]}.db'

    # Make sure DB exists by creating a version table
    service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
    service.set_tag(tag=version)

    assert _db_table_exists(database_filename=database_filename, table='version')

    md.CorpusIndexFactory(ProtocolMapper).generate(corpus_folder=corpus_folder, target_folder=target_folder)

    service.load_corpus_indexes(folder=target_folder)

    for tablename in ["protocols", "utterances", "speaker_notes"]:
        assert _db_table_exists(database_filename=database_filename, table=tablename)


# def test_load_scripts():

#     tag: str = ConfigStore().config().get("version")
#     source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'
#     script_folder: str = None
#     service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
#     service.create(tag=tag, folder=source_folder, force=True)
#     # service.load_corpus_indexes(folder=source_folder)
#     # service.load_scripts(folder=script_folder)


# def test_bugg():

#     tag: str = ConfigStore().config().get("version")
#     folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'

#     service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
#     configs: MetadataTableConfigs = MetadataTableConfigs(tag)

#     service.reset(tag=tag, force=True)
#     service.create_base_tables(configs)
#     service.load_base_tables(configs, folder)
