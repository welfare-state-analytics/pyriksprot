import logging
import os
from os.path import isdir

import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import
from loguru import logger

from pyriksprot import member
from tests.utility import (
    PARLACLARIN_SOURCE_FOLDER,
    TAGGED_SOURCE_FOLDER,
    TEST_DOCUMENTS,
    setup_parlaclarin_test_corpus,
    setup_tagged_frames_test_corpus,
)


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source=PARLACLARIN_SOURCE_FOLDER, tag=None)


if not isdir(PARLACLARIN_SOURCE_FOLDER):
    setup_parlaclarin_test_corpus(
        protocols=TEST_DOCUMENTS,
        target_folder=PARLACLARIN_SOURCE_FOLDER,
    )

if not isdir(TAGGED_SOURCE_FOLDER):
    setup_tagged_frames_test_corpus(
        protocols=TEST_DOCUMENTS,
        source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"],
        target_folder=TAGGED_SOURCE_FOLDER,
    )


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    logger.remove(handler_id)
