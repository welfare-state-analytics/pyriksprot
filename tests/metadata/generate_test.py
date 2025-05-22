import os
import pathlib
import shutil
import uuid
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

import pyriksprot.sql as sql
from pyriksprot import gitchen as gh
from pyriksprot import metadata as md
from pyriksprot.configuration import ConfigValue
from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.metadata import database
from pyriksprot.metadata.schema import MetadataSchema
from pyriksprot.workflows.create_metadata import create_database_workflow

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

    infos: dict[str, dict] = md.gh_download_folder(
        target_folder=tmp_path, user=user, repository=repository, path=path, tag=tag, force=True
    )
    assert len(infos) > 0

    shutil.rmtree(str(tmp_path))


def test_get_and_set_db_version():
    dummy_db_name: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'
    version: str = "kurt"

    service: md.MetadataFactory = md.MetadataFactory(version=version, schema=MagicMock(), filename=dummy_db_name)

    service.db.version = version
    stored_version: str = service.db.version
    assert version == stored_version
    service.verify_tag()

    version: str = "olle"
    service.db.version = version
    stored_version: str = service.db.version
    assert version == stored_version
    assert version == stored_version


def test_create_metadata_database():

    metadata_version: str = ConfigValue("metadata.version").resolve()
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{metadata_version}.db"
    metadata_folder: str = f"./tests/test_data/source/metadata/{metadata_version}"

    schema = MetadataSchema(metadata_version)

    service: md.MetadataFactory = md.MetadataFactory(version=metadata_version, filename=target_filename)

    service.create(force=True).upload(schema, metadata_folder).execute_sql_scripts()

    assert os.path.isfile(target_filename)

    service.verify_tag()

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.MetadataFactory(version=None, schema=MagicMock(), filename=target_filename).create(force=True)


def store_sql_script(tag: str) -> str:
    script: str = '\n\n'.join(
        database.SqlCompiler().to_create(tablename, cfg.all_columns_specs, cfg.constraints)
        for tablename, cfg in MetadataSchema(tag).items()
    )
    pathlib.Path(f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.sql").write_text(
        script, encoding='utf-8'
    )
    return script


@pytest.mark.skip("Infrastructure test only")
def test_create_metadata_database_DEVELOP():
    ConfigStore.configure_context(source='tests/config.yml')

    corpus_folder: str = ConfigValue("corpus.folder").resolve()
    metadata_version: str = ConfigValue("metadata.version").resolve()
    opts: dict[str, Any] = ConfigValue("metadata.database").resolve()
    metadata_folder: str = ConfigValue("metadata.folder").resolve()

    schema = MetadataSchema(metadata_version)

    md.CorpusIndexFactory(ProtocolMapper, schema=schema).generate(
        corpus_folder=corpus_folder, target_folder=metadata_folder
    )

    service: md.MetadataFactory = md.MetadataFactory(
        version=metadata_version, schema=schema, backend=opts['type'], **opts['options']
    )
    service.create(force=True).upload(schema, metadata_folder).execute_sql_scripts()
    service.verify_tag()


@pytest.mark.skip("Infrastructure test only")
def test_create_metadata_database_with_workflow():
    # ConfigStore.configure_context(source='configs/config.yml')
    # ConfigStore.configure_context(source='configs/config_postgres.yml')
    ConfigStore.configure_context(source='tests/config.yml')

    metadata_version: str = ConfigValue("metadata.version").resolve()

    db_opts: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{metadata_version}.db"
    # db_opts: dict[str, Any] = ConfigValue("metadata.database").resolve()

    create_database_workflow(
        corpus_version=ConfigValue("corpus.version").resolve(),
        metadata_version=metadata_version,
        corpus_folder=ConfigValue("corpus.folder").resolve(),
        metadata_folder=ConfigValue("metadata.folder").resolve(),
        db_opts=db_opts,
        force=True,
        skip_create_index=True,
        skip_download_metadata=True,
        skip_load_scripts=False,
    )


# def test_create_postgres_metadata_database_with_workflow():
#     # ConfigStore.configure_context(source='configs/config.yml')
#     # ConfigStore.configure_context(source='configs/config_postgres.yml')
#     os.chdir('/home/roger/source/swedeb/sample-data/data/1867-2020')
#     ConfigStore.configure_context(source='opts/pg-subset-config.yml')

#     create_database_workflow(
#         tag=ConfigValue("corpus.version").resolve(),
#         metadata_folder=ConfigValue("metadata.folder").resolve(),
#         db_opts=ConfigValue("metadata.database").resolve(),
#         corpus_folder=ConfigValue("corpus.folder").resolve(),
#         force=False,
#         skip_create_index=True,
#         skip_download_metadata=True,
#         skip_load_scripts=False,
#     )


@pytest.mark.parametrize(
    'corpus_folder',
    [ConfigValue("corpus.folder").resolve(), ConfigValue("fakes.folder").resolve()],
)
def test_generate_corpus_indexes(corpus_folder: str):
    version: str = ConfigValue("metadata.version").resolve()
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper, schema=version)
    data: dict[str, pd.DataFrame] = factory.generate(corpus_folder=corpus_folder, target_folder="tests/output").data

    assert data.get('protocols') is not None
    assert data.get('utterances') is not None
    assert data.get('speaker_notes') is not None
