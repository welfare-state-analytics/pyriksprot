import os
from posixpath import splitext

import pytest
import requests

from pyriksprot import corpus as pc
from pyriksprot import gitchen as gh
from pyriksprot import metadata as md
from pyriksprot.configuration.inject import ConfigValue

from .utility import (
    create_test_speech_corpus,
    create_test_tagged_frames_corpus,
    ensure_test_corpora_exist,
    sample_parlaclarin_corpus_exists,
    sample_tagged_frames_corpus_exists,
    subset_corpus_and_metadata,
)

jj = os.path.join

FORCE_RUN_SKIPS = False  # os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(condition=sample_parlaclarin_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus(list_of_test_protocols: list[str]):
    target_folder: str = ConfigValue("corpus.folder").resolve()
    version: str = ConfigValue("corpus.version").resolve()
    opts: dict = ConfigValue("corpus.github").resolve()

    pc.download_protocols(
        filenames=list_of_test_protocols, target_folder=target_folder, create_subfolder=True, tag=version, **opts
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_gh_fetch_metadata_by_config():
    version: str = ConfigValue("metadata.version").resolve()
    target_folder: str = jj('./metadata/data/', version)
    schema: md.MetadataSchema = md.MetadataSchema(version)
    md.gh_fetch_metadata_by_config(schema=schema, tag=version, folder=target_folder, force=True, errors='raise')
    assert all(
        os.path.isfile(jj(target_folder, cfg.basename)) for cfg in schema.definitions.values() if not cfg.is_derived
    )


def test_gh_fetch_metadata_folder():
    version: str = ConfigValue("metadata.version").resolve()
    user: str = ConfigValue("metadata.github.user").resolve()
    repository: str = ConfigValue("metadata.github.repository").resolve()
    path: str = ConfigValue("metadata.github.path").resolve()

    target_folder: str = jj('./metadata/data/', version)

    md.gh_fetch_metadata_folder(
        target_folder=target_folder, user=user, repository=repository, path=path, tag=version, force=True
    )

    assert True


def test_gh_fetch_metadata_folder_old():
    version: str = "v0.14.0"
    user: str = "welfare-state-analytics"
    repository: str = "riksdagen-corpus"
    path: str = "corpus/metadata"

    target_folder: str = jj('./metadata/data/full', version)

    md.gh_fetch_metadata_folder(
        target_folder=target_folder, user=user, repository=repository, path=path, tag=version, force=True
    )

    assert True


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_frames_corpus_exists(), reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus(list_of_test_protocols: list[str]):
    version: str = ConfigValue("version").resolve()
    data_folder: str = ConfigValue("data_folder").resolve()
    tagged_folder: str = ConfigValue("tagged_frames.folder").resolve()
    riksprot_tagged_folder: str = jj(data_folder, version, 'tagged_frames')

    create_test_tagged_frames_corpus(
        protocols=list_of_test_protocols,
        source_folder=riksprot_tagged_folder,
        target_folder=tagged_folder,
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_subset_corpus_and_metadata(list_of_test_protocols: list[str]):
    version: str = ConfigValue("metadata.version").resolve()
    data_folder: str = ConfigValue("data_folder").resolve()
    gh_metadata_opts: dict[str, str] = ConfigValue("metadata.github").resolve()
    gh_records_opts: dict[str, str] = ConfigValue("corpus.github").resolve()
    source_folder: str = ConfigValue("global.corpus.folder").resolve()
    db_opts: dict[str, str] = ConfigValue("metadata.database").resolve()

    subset_corpus_and_metadata(
        source_folder=source_folder,
        tag=version,
        target_folder=data_folder,
        documents=list_of_test_protocols,
        gh_metadata_opts=gh_metadata_opts,
        gh_records_opts=gh_records_opts,
        db_opts=db_opts,
    )


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_sample_speech_corpora():
    version: str = ConfigValue("version").resolve()
    tagged_folder: str = ConfigValue("tagged_frames.folder").resolve()
    database: str = ConfigValue("metadata.database.options.filename").resolve()
    create_test_speech_corpus(source_folder=tagged_folder, tag=version, database_name=database)


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist(force=True)


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_gh_tables_data():
    tag: str = ConfigValue("metadata.version").resolve()
    cfg: dict[str, str] = ConfigValue("metadata.github").resolve() or {}

    user: str = cfg.get('user')
    repository: str = cfg.get('repository')
    path: str = cfg.get('path')

    items: list[dict] = gh.gh_ls(user=user, repository=repository, path=path, tag=tag)

    infos: dict[str, dict] = {}
    for item in items:
        table, extension = splitext(item.get("name", ""))
        if not extension.endswith("csv"):
            continue
        url: str = gh.gh_create_url(
            user=cfg.get('user'),
            repository=cfg.get('repository'),
            path=cfg.get('path'),
            filename=f"{item.get('name')}.csv",
            tag=tag,
        )
        data: str = requests.get(url, timeout=10).content.decode("utf-8")
        headers: list[str] = data.splitlines()[0].split(sep=',')
        infos['table'] = {'name': table, 'headers': headers, 'content': data}
    return infos
