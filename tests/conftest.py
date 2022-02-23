from __future__ import annotations

import logging
import os
from os.path import isdir
from os.path import join as jj

import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import
from loguru import logger

from pyriksprot import download_metadata, download_protocols, member
from tests.utility import (
    PARLACLARIN_SOURCE_FOLDER,
    PARLACLARIN_SOURCE_TAG,
    TAGGED_SOURCE_FOLDER,
    TEST_DOCUMENTS,
    sample_metadata_exists,
    sample_xml_corpus_exists,
    setup_sample_tagged_frames_corpus,
)


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source=PARLACLARIN_SOURCE_FOLDER, tag=None)


if not sample_metadata_exists():
    target_folder: str = jj(PARLACLARIN_SOURCE_FOLDER, "metadata")
    download_metadata("metadata", PARLACLARIN_SOURCE_TAG)

if not sample_xml_corpus_exists():
    protocols: list[str] = TEST_DOCUMENTS
    target_folder: str = jj(PARLACLARIN_SOURCE_FOLDER, "protocols")
    download_protocols(
        protocols=protocols, target_folder=target_folder, create_subfolder=True, tag=PARLACLARIN_SOURCE_TAG
    )


if not isdir(TAGGED_SOURCE_FOLDER):
    try:
        setup_sample_tagged_frames_corpus(
            protocols=TEST_DOCUMENTS,
            source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"],
            target_folder=TAGGED_SOURCE_FOLDER,
        )
    except Exception as ex:
        logger.warning(ex)


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    logger.remove(handler_id)
