import pytest

from pyriksprot import member

TEST_CORPUS_FOLDER = 'tests/test_data/source'


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(source_folder=f'{TEST_CORPUS_FOLDER}', branch='dev')
