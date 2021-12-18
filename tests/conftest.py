from os.path import isdir

import pytest

from pyriksprot import member

from .utility import PARLACLARIN_SOURCE_FOLDER, create_parlaclarin_corpus


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source=PARLACLARIN_SOURCE_FOLDER, tag=None)


if not isdir(PARLACLARIN_SOURCE_FOLDER):
    create_parlaclarin_corpus(PARLACLARIN_SOURCE_FOLDER)
