import pandas as pd

from pyriksprot import member as pm


def test_load_parliamentary_members():
    persons: pm.ParliamentaryRole = pm.ParliamentaryMemberIndex.load_members(pm.members_of_parliament_url(branch='dev'))

    assert len(persons) > 0

    assert 'adam_hult_d8d379' in persons.index
    assert 'magdalena_andersson_finansminist' not in persons.index


def test_load_ministers():
    party_abbrevs: dict = {}
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex.load_ministers(pm.ministers_url(branch='dev'), party_abbrevs)

    assert len(persons) > 0

    assert 'magdalena_andersson_finansminist' in persons.index
    assert 'adam_hult_d8d379' not in persons.index


def test_load_speakers():
    party_abbrevs: dict = {}
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex.load_speakers(pm.speakers_url(branch='dev'), party_abbrevs)

    assert len(persons) > 0

    assert 'magdalena_andersson_finansminist' not in persons.index
    assert 'adam_hult_d8d379' not in persons.index
    assert 'anders_bjorck_1_vice_talman' in persons.index


def test_load_roles():
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex(branch='dev')

    assert len(persons) > 0

    assert 'magdalena_andersson_finansminist' in persons
    assert 'adam_hult_d8d379' in persons
