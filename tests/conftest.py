from os.path import isdir

import pytest

from pyriksprot import member

from .utility import PARLACLARIN_SOURCE_BRANCH, PARLACLARIN_SOURCE_FOLDER, create_parlaclarin_corpus


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source_folder=PARLACLARIN_SOURCE_FOLDER, branch=PARLACLARIN_SOURCE_BRANCH)


if not isdir(PARLACLARIN_SOURCE_FOLDER):
    create_parlaclarin_corpus(PARLACLARIN_SOURCE_FOLDER)
