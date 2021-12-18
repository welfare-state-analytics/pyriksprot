import pandas as pd

from pyriksprot import member as pm

from .utility import PARLACLARIN_SOURCE_FOLDER


def test_load_parliamentary_members():
    persons: pm.ParliamentaryRole = pm.ParliamentaryMemberIndex.load_members(source=PARLACLARIN_SOURCE_FOLDER, tag=None)

    assert len(persons) > 0

    assert 'adam_hult_d8d379' in persons.index
    assert 'magdalena_andersson_minister_2021' not in persons.index


def test_load_ministers():
    party_abbrevs: dict = {}
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex.load_ministers(
        source=PARLACLARIN_SOURCE_FOLDER, party_abbrevs=party_abbrevs
    )

    assert len(persons) > 0

    assert 'magdalena_andersson_minister_2021' in persons.index
    assert 'adam_hult_d8d379' not in persons.index


def test_load_speakers():
    party_abbrevs: dict = {}
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex.load_speakers(
        source=PARLACLARIN_SOURCE_FOLDER, party_abbrevs=party_abbrevs
    )

    assert len(persons) > 0

    assert 'magdalena_andersson_minister_2021' not in persons.index
    assert 'adam_hult_d8d379' not in persons.index
    assert 'anders_bjorck_1_vice_talman' in persons.index


def test_load_roles():
    persons: pd.DataFrame = pm.ParliamentaryMemberIndex(source=PARLACLARIN_SOURCE_FOLDER)

    assert len(persons) > 0

    assert 'magdalena_andersson_minister_2021' in persons
    assert 'adam_hult_d8d379' in persons
