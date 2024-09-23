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

    infos: dict[str, dict] = md.gh_fetch_metadata_folder(
        target_folder=tmp_path, user=user, repository=repository, path=path, tag=tag, force=True
    )
    assert len(infos) > 0

    shutil.rmtree(str(tmp_path))


def test_get_and_set_db_version():
    dummy_db_name: str = f'./tests/output/{str(uuid.uuid4())[:8]}.md'
    tag: str = "kurt"

    service: md.MetadataFactory = md.MetadataFactory(tag=tag, schema=MagicMock(), filename=dummy_db_name)

    service.db.version = tag
    stored_tag: str = service.db.version
    assert tag == stored_tag
    service.verify_tag()

    tag: str = "olle"
    service.db.version = tag
    stored_tag: str = service.db.version
    assert tag == stored_tag
    assert tag == stored_tag


def test_create_metadata_database():

    tag: str = ConfigValue("metadata.version").resolve()
    target_filename: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    metadata_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
    corpus_folder: str = ConfigValue("corpus.folder").resolve()

    schema = MetadataSchema(tag)

    md.CorpusIndexFactory(ProtocolMapper, schema=schema).generate(
        corpus_folder=corpus_folder, target_folder=metadata_folder
    )

    service: md.MetadataFactory = md.MetadataFactory(tag=tag, filename=target_filename)

    service.create(force=True).upload(schema, metadata_folder).execute_sql_scripts()

    assert os.path.isfile(target_filename)

    service.verify_tag()

    os.remove(target_filename)

    with pytest.raises(ValueError):
        md.MetadataFactory(tag=None, schema=MagicMock(), filename=target_filename).create(force=True)


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
    # ConfigStore.configure_context(source='configs/config.yml')
    # ConfigStore.configure_context(source='configs/config_postgres.yml')
    ConfigStore.configure_context(source='tests/config.yml')

    corpus_folder: str = ConfigValue("corpus.folder").resolve()

    tag: str = "v1.1.0"

    opts: dict[str, Any] = ConfigValue("metadata.database").resolve()
    metadata_folder: str = ConfigValue("metadata.folder").resolve()

    schema = MetadataSchema(tag)

    md.CorpusIndexFactory(ProtocolMapper, schema=schema).generate(
        corpus_folder=corpus_folder, target_folder=metadata_folder
    )

    service: md.MetadataFactory = md.MetadataFactory(tag=tag, schema=schema, backend=opts['type'], **opts['options'])
    service.create(force=True).upload(schema, metadata_folder).execute_sql_scripts()
    service.verify_tag()


@pytest.mark.skip("Infrastructure test only")
def test_create_metadata_database_with_workflow():
    # ConfigStore.configure_context(source='configs/config.yml')
    # ConfigStore.configure_context(source='configs/config_postgres.yml')
    ConfigStore.configure_context(source='tests/config.yml')

    corpus_folder: str = ConfigValue("corpus.folder").resolve()
    metadata_folder: str = ConfigValue("metadata.folder").resolve()
    tag: str = ConfigValue("corpus.version").resolve()

    db_opts: str = f"./tests/output/{str(uuid.uuid4())[:8]}_riksprot_metadata.{tag}.db"
    # db_opts: dict[str, Any] = ConfigValue("metadata.database").resolve()

    create_database_workflow(
        tag=tag,
        metadata_folder=metadata_folder,
        db_opts=db_opts,
        corpus_folder=corpus_folder,
        force=True,
        skip_create_index=True,
        skip_download_metadata=True,
        skip_load_scripts=False,
    )


@pytest.mark.parametrize(
    'corpus_folder',
    [ConfigValue("corpus.folder").resolve(), ConfigValue("fakes.folder").resolve()],
)
def test_generate_corpus_indexes(corpus_folder: str):
    tag: str = ConfigValue("metadata.version").resolve()
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper, schema=tag)
    data: dict[str, pd.DataFrame] = factory.generate(corpus_folder=corpus_folder, target_folder="tests/output").data

    assert data.get('protocols') is not None
    assert data.get('utterances') is not None
    assert data.get('speaker_notes') is not None


# def test_load_scripts():

#     tag: str = ConfigValue("version").resolve()
#     source_folder: str = f"./tests/test_data/source/{tag}/parlaclarin/metadata"
#     database_filename: str = f'./tests/output/{str(uuid.uuid4())[:10]}.db'
#     script_folder: str = None
#     service: md.MetadataFactory = md.MetadataFactory(tag=tag, filename=database_filename)
#     service.create(folder=source_folder, force=True)
#     # service.load_corpus_indexes(folder=source_folder)
#     # service.load_scripts(folder=script_folder)
