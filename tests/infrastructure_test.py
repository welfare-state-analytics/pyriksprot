import os
from os.path import splitext

import pytest
import requests

from pyriksprot import corpus as pc
from pyriksprot import gitchen as gh
from pyriksprot import metadata as md
from pyriksprot.configuration.inject import ConfigValue
from pyriksprot.metadata.database import DatabaseInterface
from pyriksprot.workflows.create_metadata import resolve_backend

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
    md.gh_download_by_config(schema=schema, version=version, folder=target_folder, force=True, errors='raise')
    assert all(
        os.path.isfile(jj(target_folder, cfg.basename)) for cfg in schema.definitions.values() if not cfg.is_derived
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_frames_corpus_exists(), reason="Test infrastructure test")
def test_gh_fetch_metadata_folder():
    version: str = ConfigValue("metadata.version").resolve()
    gh_opts: dict[str, str] = ConfigValue("metadata.github").resolve()
    target_folder: str = jj('./metadata/data/', version)

    md.gh_download_folder(target_folder=target_folder, **gh_opts, tag=version, force=True)

    assert True


def test_gh_download_items():
    tag: str = ConfigValue("metadata.version").resolve()
    folder: str = 'tests/output'

    urls: dict[str, str] = {
        'swedeb-parties.csv': "https://raw.githubusercontent.com/humlab-swedeb/sample-data/refs/heads/main/data/resources/swedeb-parties.csv"
    }

    items: dict[str, dict[str, str]] = md.gh_download_files(folder, tag, errors='raise', items=urls)

    assert len(items) == len(urls)

    assert all(os.path.isfile(jj(folder, x)) for x in items)


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_frames_corpus_exists(), reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus(list_of_test_protocols: list[str]):
    corpus_version: str = ConfigValue("corpus:version").resolve()
    data_folder: str = ConfigValue("data_folder").resolve()
    tagged_folder: str = ConfigValue("tagged_frames.folder").resolve()
    riksprot_tagged_folder: str = jj(data_folder, corpus_version, 'tagged_frames')

    create_test_tagged_frames_corpus(
        protocols=list_of_test_protocols,
        source_folder=riksprot_tagged_folder,
        target_folder=tagged_folder,
    )


# @pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_subset_corpus_and_metadata(list_of_test_protocols: list[str]):

    subset_corpus_and_metadata(
        corpus_version=ConfigValue("corpus.version").resolve(),
        metadata_version=ConfigValue("metadata.version").resolve(),
        corpus_folder=ConfigValue("corpus.folder").resolve(),
        metadata_folder=ConfigValue("metadata.folder").resolve(),
        documents=list_of_test_protocols,
        global_corpus_folder=ConfigValue("global.corpus.folder").resolve(),
        global_metadata_folder=ConfigValue("global.metadata.folder").resolve(),
        target_root_folder=ConfigValue("data_folder").resolve(),
        scripts_folder=None,
        gh_metadata_opts=ConfigValue("metadata.github").resolve(),
        gh_records_opts=ConfigValue("corpus.github").resolve(),
        db_opts=ConfigValue("metadata.database").resolve(),
        tf_filename=ConfigValue("dehyphen.tf_filename").resolve(),
        skip_download=True,
        force=True,
    )

    db: DatabaseInterface = resolve_backend(ConfigValue("metadata.database").resolve())

    assert db is not None

    assert db.fetch_scalar("SELECT COUNT(*) FROM _person") > 0


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_sample_speech_corpora():
    corpus_version: str = ConfigValue("corpus:version").resolve()
    tagged_folder: str = ConfigValue("tagged_frames.folder").resolve()
    database: str = ConfigValue("metadata.database.options.filename").resolve()
    create_test_speech_corpus(source_folder=tagged_folder, corpus_version=corpus_version, database_name=database)


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist(only_check=False)


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
