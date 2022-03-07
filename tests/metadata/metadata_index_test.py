from pyriksprot import metadata as md
from pyriksprot.metadata.person import index_of_person_id, swap_rows

import pandas as pd
import pytest

DATABASE_FILENAME: str = "./metadata/riksprot_metadata.v0.4.0.db"


@pytest.fixture
def person_index() -> md.PersonIndex:
    return md.PersonIndex(DATABASE_FILENAME).load()


def test_code_lookups():
    lookups: md.Codecs = md.Codecs().load(DATABASE_FILENAME)
    assert lookups

    assert lookups.gender2id.get("woman") == 2
    assert lookups.gender2name.get(2) == "woman"

    assert lookups.office_type2id.get("unknown") == 0
    assert lookups.office_type2id.get("Minister") == 2
    assert lookups.office_type2name.get(2) == "Minister"

    assert lookups.sub_office_type2name.get(0) == "unknown"
    assert lookups.sub_office_type2name.get(1) == "Ledamot av första kammaren"
    assert lookups.sub_office_type2id.get("Ledamot av första kammaren") == 1

    assert lookups.party_abbrev2name.get(0) == "?"

    df: pd.DataFrame = pd.DataFrame(
        data=dict(
            gender_id=[0, 1, 2],
            party_id=[0, 5, 3],
            office_type_id=[0, 2, 3],
            sub_office_type_id=[0, 2, 8],
        )
    )


def test_load():

    database_filename: str = "./metadata/riksprot_metadata.v0.4.0.db"
    person_index: md.PersonIndex = md.PersonIndex(database_filename).load()

    assert person_index is not None

    expected_data: dict = {
        'person_id': 'Q3358763',
        'name': 'Monica Green',
        'gender_id': 2,
        'party_id': 8,
        'year_of_birth': 1959,
        'year_of_death': 0,
    }

    person: md.Person = person_index.get_person(500)
    assert person is not None
    assert expected_data == {k: v for k, v in person.__dict__.items() if k in expected_data}

    person: md.Person = person_index.get_person('Q3358763')
    assert person is not None
    assert expected_data == {k: v for k, v in person.__dict__.items() if k in expected_data}

    person_lookup = person_index.person_lookup
    assert person_lookup is not None
    person: md.Person = person_lookup['Q3358763']
    assert expected_data == {k: v for k, v in person.__dict__.items() if k in expected_data}

    terms: list[md.TermOfOffice] = person_index.terms_of_office_lookup.get('Q3358763')

    assert (
        terms
        == person.terms_of_office
        == [
            md.TermOfOffice(office_type_id=1, sub_office_type_id=4, **x)
            for x in [
                {'start_year': 1994, 'end_year': 1998},
                {'start_year': 1998, 'end_year': 2002},
                {'start_year': 2002, 'end_year': 2006},
                {'start_year': 2006, 'end_year': 2010},
                {'start_year': 2010, 'end_year': 2014},
                {'start_year': 2014, 'end_year': 2018},
            ]
        ]
    )

    Q6093334_pid = index_of_person_id(person_index.persons, 'Q6093334')
    Q5967602_pid = index_of_person_id(person_index.persons, 'Q5967602')

    Q6093334_data = person_index.persons.iloc[Q6093334_pid].copy()
    Q5967602_data = person_index.persons.iloc[Q5967602_pid].copy()

    swap_rows(person_index.persons, Q6093334_pid, Q5967602_pid)

    assert (person_index.persons.iloc[Q6093334_pid].copy() == Q5967602_data).all()
    assert (person_index.persons.iloc[Q5967602_pid].copy() == Q6093334_data).all()


def test_person_with_ambiguous_party():
    person: md.Person = md.Person(
        person_id='Q1347810',
        name='Kilbom',
        gender_id=1,
        party_id=0,
        year_of_birth=1885,
        year_of_death=1961,
        terms_of_office=[
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1922, end_year=1924),
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1929, end_year=1944),
        ],
        alt_parties=[
            md.PersonParty(party_id=8, start_year=0, end_year=0),
            md.PersonParty(party_id=1, start_year=0, end_year=0),
            md.PersonParty(party_id=10, start_year=0, end_year=0),
        ],
    )


def test_fetch_person():
    database_filename: str = "./metadata/riksprot_metadata.v0.4.0.db"
    service = md.SpeekerInfoService(database_filename)

    person: md.Person = service.person_index.get_person('Q1347810')
    assert person.alt_parties
    assert len(person.alt_parties) == 3
    assert set(a.party_id for a in person.alt_parties) == {8, 1, 10}
    assert set(a.start_year for a in person.alt_parties) == {0}
    assert set(a.end_year for a in person.alt_parties) == {0}
    assert person.party_at(1950) == 0


def test_speaker_info_repository():
    database_filename: str = "./metadata/riksprot_metadata.v0.4.0.db"
    service = md.SpeekerInfoService(database_filename)

    person: md.Person = service.person_index.get_person('Q1347810')
    assert person
