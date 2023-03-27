import os
import sqlite3
import uuid
from contextlib import closing

import pandas as pd
import pytest

import pyriksprot.sql as sql
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper

jj = os.path.join

RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]

DUMMY_METADATA_DATABASE_NAME: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'


def test_list_sql_files():
    data = sql.sql_file_paths()
    assert len(data) > 0


def test_gh_ls():

    data: list[dict] = md.gh_ls(
        "welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", RIKSPROT_REPOSITORY_TAG
    )
    assert len(data) > 0


def test_download_metadata():

    data: list[dict] = md.gh_ls(
        "welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", RIKSPROT_REPOSITORY_TAG
    )
    assert len(data) > 0

    filenames: list[str] = md.gh_dl_metadata_extra(folder="./tests/output", tag=RIKSPROT_REPOSITORY_TAG, force=True)
    assert len(filenames) > 0


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
    tag: str = RIKSPROT_REPOSITORY_TAG
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
    service: md.DatabaseHelper = md.DatabaseHelper(target_filename)
    service.create(tag=tag, folder=source_folder, force=True)

    assert os.path.isfile(target_filename)

    service.verify_tag(tag=tag)

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.DatabaseHelper(target_filename).create(tag=None, folder=source_folder, force=True)


def test_generate_corpus_indexes():
    corpus_folder: str = jj("./tests/test_data/source", RIKSPROT_REPOSITORY_TAG, "parlaclarin")

    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    data: dict[str, pd.DataFrame] = factory.generate(corpus_folder=corpus_folder).data

    assert data.get('protocols') is not None
    assert data.get('utterances') is not None
    assert data.get('speaker_notes') is not None


def _db_table_exists(database_filename: str, table: str):
    with closing(sqlite3.connect(database_filename)) as db:
        with closing(db.cursor()) as cursor:
            cursor.execute(f"select count(name) from sqlite_master where type='table' and name='{table}'")
            return cursor.fetchone()[0] == 1


def test_generate_and_load_corpus_indexes():
    corpus_folder: str = jj("./tests/test_data/source", RIKSPROT_REPOSITORY_TAG, "parlaclarin")
    target_folder: str = f"./tests/output/{str(uuid.uuid4())[:8]}"
    database_filename: str = f'./tests/output/{str(uuid.uuid4())[:8]}.db'

    # Make sure DB exists by creating a version table
    service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
    service.set_tag(tag=RIKSPROT_REPOSITORY_TAG)

    assert _db_table_exists(database_filename=database_filename, table='version')

    md.CorpusIndexFactory(ProtocolMapper).generate(corpus_folder=corpus_folder, target_folder=target_folder)

    service.load_corpus_indexes(folder=target_folder)

    for tablename in ["protocols", "utterances", "speaker_notes"]:
        assert _db_table_exists(database_filename=database_filename, table=tablename)


# def test_load_scripts():

#     tag: str = RIKSPROT_REPOSITORY_TAG
#     source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'
#     script_folder: str = None
#     service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
#     service.create(tag=tag, folder=source_folder, force=True)
#     # service.load_corpus_indexes(folder=source_folder)
#     # service.load_scripts(folder=script_folder)


# def test_bugg():

#     tag: str = RIKSPROT_REPOSITORY_TAG
#     folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'

#     service: md.DatabaseHelper = md.DatabaseHelper(database_filename)
#     configs: MetadataTableConfigs = MetadataTableConfigs()

#     service.reset(tag=tag, force=True)
#     service.create_base_tables(configs)
#     service.load_base_tables(configs, folder)
