import os
from os.path import isdir

import pytest

from pyriksprot import member

from .utility import (
    PARLACLARIN_SOURCE_FOLDER,
    TAGGED_SOURCE_FOLDER,
    setup_parlaclarin_test_corpus,
    setup_tagged_frames_test_corpus,
)


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source=PARLACLARIN_SOURCE_FOLDER, tag=None)


if not isdir(PARLACLARIN_SOURCE_FOLDER):
    setup_parlaclarin_test_corpus(PARLACLARIN_SOURCE_FOLDER)

if not isdir(TAGGED_SOURCE_FOLDER):
    setup_tagged_frames_test_corpus(source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"])
