import os
import pathlib
import shutil
import uuid

import pandas as pd
import pytest

import pyriksprot.sql as sql
from pyriksprot import gitchen as gh
from pyriksprot import metadata as md
from pyriksprot.configuration import ConfigValue
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.metadata import database
from pyriksprot.metadata.schema import MetadataSchema
from tests.utility import ensure_test_corpora_exist

jj = os.path.join

DUMMY_METADATA_DATABASE_NAME: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'


def test_list_sql_files():
    tag: str = ConfigValue("metadata.version").resolve()
    data = sql.sql_file_paths(tag=tag)
    assert len(data) > 0


def test_gh_ls():
    tag: str = ConfigValue("metadata.version").resolve()
    user: str = ConfigValue("metadata.github.user").resolve()
    repository: str = ConfigValue("metadata.github.repository").resolve()
    path: str = ConfigValue("metadata.github.path").resolve()

    data: list[dict] = gh.gh_ls(user=user, repository=repository, path=path, tag=tag)
    assert len(data) > 0


def test_gh_fetch_metadata_folder(tmp_path: pathlib.Path):
    tag: str = ConfigValue("metadata.version").resolve()
    user: str = ConfigValue("metadata.github.user").resolve()
    repository: str = ConfigValue("metadata.github.repository").resolve()
    path: str = ConfigValue("metadata.github.path").resolve()

    data: list[dict] = gh.gh_ls(user=user, repository=repository, path=path, tag=tag)
    assert len(data) > 0

    filenames: list[str] = md.gh_fetch_metadata_folder(
        target_folder=tmp_path, user=user, repository=repository, path=path, tag=tag, force=True
    )
    assert len(filenames) > 0

    shutil.rmtree(str(tmp_path))


def test_get_and_set_db_version():
    dummy_db_name: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'

    service: md.GenerateService = md.GenerateService(filename=dummy_db_name)

    tag: str = "kurt"
    service.db.version = tag
    stored_tag: str = service.db.version
    assert tag == stored_tag
    service.verify_tag(tag=tag)

    tag: str = "olle"
    service.db.version = tag
    stored_tag: str = service.db.version
    assert tag == stored_tag
    assert tag == stored_tag


def test_create_metadata_database():
    ensure_test_corpora_exist(force=True)

    tag: str = ConfigValue("metadata.version").resolve()
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
    service: md.GenerateService = md.GenerateService(filename=target_filename)
    service.create(tag=tag, folder=source_folder, force=True)

    assert os.path.isfile(target_filename)

    service.verify_tag(tag=tag)

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.GenerateService(filename=target_filename).create(tag=None, folder=source_folder, force=True)


def store_sql_script(tag: str) -> str:
    script: str = '\n\n'.join(
        database.SqlCompiler().to_create(tablename, cfg.all_columns_specs, cfg.constraints)
        for tablename, cfg in MetadataSchema(tag).items()
    )
    pathlib.Path(f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.sql").write_text(
        script, encoding='utf-8'
    )
    return script


def test_create_metadata_database_DEVELOP():
    tag: str = "v1.1.0"

    # store_sql_script(tag)

    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    source_folder: str = f"./metadata/data/{tag}"
    service: md.GenerateService = md.GenerateService(filename=target_filename)
    service.create(tag=tag, folder=source_folder, force=True)

    assert os.path.isfile(target_filename)

    service.verify_tag(tag=tag)

    service.execute_sql_scripts(folder=None, tag=tag)

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.GenerateService(filename=target_filename).create(tag=None, folder=source_folder, force=True)


@pytest.mark.parametrize(
    'corpus_folder',
    [ConfigValue("corpus.folder").resolve(), ConfigValue("fakes.folder").resolve()],
)
def test_generate_corpus_indexes(corpus_folder: str):
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    data: dict[str, pd.DataFrame] = factory.generate(corpus_folder=corpus_folder, target_folder="tests/output").data

    assert data.get('protocols') is not None
    assert data.get('utterances') is not None
    assert data.get('speaker_notes') is not None


def _db_table_exists(database_filename: str, table: str):
    with database.DefaultDatabaseType(filename=database_filename) as db:
        return db.fetch_scalar(f"select count(name) from sqlite_master where type='table' and name='{table}'") == 1


def test_generate_and_load_corpus_indexes():
    version: str = ConfigValue("metadata.version").resolve()
    corpus_folder: str = jj("./tests/test_data/source", version, "parlaclarin")
    target_folder: str = f"./tests/output/{str(uuid.uuid4())[:8]}"
    database_filename: str = f'./tests/output/{str(uuid.uuid4())[:8]}.db'

    # Make sure DB exists by creating a version table
    service: md.GenerateService = md.GenerateService(filename=database_filename)
    service.db.version = version

    assert _db_table_exists(database_filename=database_filename, table='version')

    md.CorpusIndexFactory(ProtocolMapper).generate(corpus_folder=corpus_folder, target_folder=target_folder)

    service.upload_corpus_indexes(folder=target_folder)

    for tablename in ["protocols", "utterances", "speaker_notes"]:
        assert _db_table_exists(database_filename=database_filename, table=tablename)


# def test_load_scripts():

#     tag: str = ConfigValue("version").resolve()
#     source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'
#     script_folder: str = None
#     service: md.GenerateService = md.GenerateService(database_filename)
#     service.create(tag=tag, folder=source_folder, force=True)
#     # service.load_corpus_indexes(folder=source_folder)
#     # service.load_scripts(folder=script_folder)


# def test_bugg():

#     tag: str = ConfigStore().config().get("version")
#     folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'

#     service: md.GenerateService = md.GenerateService(database_filename)
#     configs: MetadataSchema = MetadataSchema(tag)

#     service.reset(tag=tag, force=True)
#     service.create_base_tables(configs)
#     service.load_base_tables(configs, folder)
