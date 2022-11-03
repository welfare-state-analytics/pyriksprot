from contextlib import closing
import os
import sqlite3
import uuid
import pytest
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper

jj = os.path.join

RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]

DUMMY_METADATA_DATABASE_NAME: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'


def test_get_and_set_db_version():

    dummy_db_name: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'

    tag: str = "kurt"
    db: sqlite3.Connection = sqlite3.connect(dummy_db_name)
    md.set_db_tag(path_or_db=db, tag=tag)
    stored_tag: str = md.get_db_tag(path_or_db=db)
    assert tag == stored_tag
    md.assert_db_tag(path_or_db=db, tag=tag)

    tag: str = "olle"
    md.set_db_tag(path_or_db=db, tag=tag)
    stored_tag: str = md.get_db_tag(path_or_db=db)
    assert tag == stored_tag
    md.assert_db_tag(path_or_db=db, tag=tag)


def test_create_metadata_database():
    tag: str = RIKSPROT_REPOSITORY_TAG
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"

    md.create_database(database_filename=target_filename, tag=tag, folder=source_folder, force=True)
    assert os.path.isfile(target_filename)
    md.assert_db_tag(path_or_db=target_filename, tag=tag)

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.create_database(database_filename=target_filename, tag=None, folder=source_folder, force=True)


def test_generate_corpus_indexes():
    corpus_folder: str = jj("./tests/test_data/source", RIKSPROT_REPOSITORY_TAG, "parlaclarin")
    protocols, utterances, speaker_notes = md.generate_corpus_indexes(ProtocolMapper, corpus_folder)
    assert protocols is not None
    assert utterances is not None
    assert speaker_notes is not None


def test_generate_and_load_corpus_indexes():
    corpus_folder: str = jj("./tests/test_data/source", RIKSPROT_REPOSITORY_TAG, "parlaclarin")
    target_folder: str = f"./tests/output/{str(uuid.uuid4())[:8]}"
    database_filename: str = f'./tests/output/{str(uuid.uuid4())[:8]}.db'

    # Make sure DB exists by creating a version table
    md.set_db_tag(path_or_db=database_filename, tag=RIKSPROT_REPOSITORY_TAG)
    assert md.db_table_exists(database_filename=database_filename, table='version')

    md.generate_corpus_indexes(ProtocolMapper, corpus_folder=corpus_folder, target_folder=target_folder)

    md.load_corpus_indexes(database_filename=database_filename, source_folder=target_folder)
    for tablename in ["protocols", "utterances", "speaker_notes"]:
        assert md.db_table_exists(database_filename=database_filename, table=tablename)


def test_load_scripts():
    
    tag: str = RIKSPROT_REPOSITORY_TAG
    source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
    database_filename: str = f'./tests/output/{str(uuid.uuid4())[:8]}.db'
    script_folder: str = "./metadata/sql"

    md.create_database(database_filename=database_filename, tag=tag, folder=source_folder, force=True)
    md.load_corpus_indexes(database_filename=database_filename, source_folder=source_folder)

    md.load_scripts(database_filename=database_filename, script_folder=script_folder)
