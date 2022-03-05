from __future__ import annotations

import logging

import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import
from loguru import logger

from .utility import ensure_test_corpora_exist

ensure_test_corpora_exist()


@pytest.fixture
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    logger.remove(handler_id)
