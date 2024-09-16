from dataclasses import asdict

import pandas as pd
import pytest

from pyriksprot import metadata as md
from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.metadata.person import index_of_person_id, swap_rows

# pylint: disable=redefined-outer-name


def dummy() -> md.Person:
    return md.Person(
        pid=45,
        person_id='Q1347810',
        name='Kilbom',
        gender_id=1,
        party_id=0,
        year_of_birth=1885,
        year_of_death=1961,
        terms_of_office=[
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_date=1922, end_date=1924),
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_date=1929, end_date=1944),
        ],
        alt_parties=[
            md.PersonParty(party_id=8, start_date=0, end_date=0),
            md.PersonParty(party_id=1, start_date=0, end_date=0),
            md.PersonParty(party_id=10, start_date=0, end_date=0),
        ],
    )


def test_code_lookups():
    database: str = ConfigStore.config().get("metadata.database.options.filename")

    assert database is not None

    lookups: md.Codecs = md.Codecs().load(database)
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
            party_id=[0, 6, 3],
            office_type_id=[0, 2, 3],
            sub_office_type_id=[
                0,
                lookups.sub_office_type2id.get('finansminister'),
                lookups.sub_office_type2id.get('andra kammarens andre vice talman'),
            ],
        )
    )
    id_columns: set[str] = {'gender_id', 'party_id', 'office_type_id', 'sub_office_type_id'}
    name_columns: set[str] = {'gender', 'party_abbrev', 'office_type', 'sub_office_type'}
    df_decoded: pd.DataFrame = lookups.decode(df, drop=False)

    assert set(df_decoded.columns) == id_columns.union(name_columns)
    assert (df_decoded.party_abbrev == ['?', 'MP', 'KD']).all()
    assert (df_decoded.gender == ['unknown', 'man', 'woman']).all()
    assert (df_decoded.office_type == ['unknown', 'Minister', 'Talman']).all()
    assert (df_decoded.sub_office_type == ['unknown', 'finansminister', 'andra kammarens andre vice talman']).all()

    df_decoded: pd.DataFrame = lookups.decode(df, drop=True)
    assert set(df_decoded.columns) == name_columns

    assert lookups.property_values_specs and len(lookups.property_values_specs) == 4


def test_person_index(person_index: md.PersonIndex):
    person_id: str = 'Q5556026'
    pid: int = person_index.person_id2pid.get(person_id)
    expected_data: dict = {
        'person_id': person_id,
        'name': 'Sten Andersson',
        'gender_id': 1,
        'party_id': 0,
        'year_of_birth': 1943,
        'year_of_death': 2010,
    }

    person: md.Person = person_index.get_person(person_id)
    assert expected_data == {k: v for k, v in asdict(person).items() if k in expected_data}

    person: md.Person = person_index.get_person(pid)
    assert expected_data == {k: v for k, v in asdict(person).items() if k in expected_data}

    person_lookup = person_index.person_lookup
    person: md.Person = person_lookup[person_id]
    assert expected_data == {k: v for k, v in asdict(person).items() if k in expected_data}

    terms: list[md.TermOfOffice] = person_index.terms_of_office_lookup.get(person_id)

    assert (
        sorted(terms, key=lambda x: x.start_year)
        == sorted(person.terms_of_office, key=lambda x: x.start_year)
        == sorted(
            [
                # Q5556026,1983-05-31,1985-09-30,,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1983-05-31",
                    end_date="1985-09-30",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,1985-09-30,1988-10-03,Fyrstadskretsen,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1985-09-30",
                    end_date="1988-10-03",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,1988-10-03,1991-09-30,Fyrstadskretsen,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1988-10-03",
                    end_date="1991-09-30",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,1991-09-30,1994-10-03,Fyrstadskretsen,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1991-09-30",
                    end_date="1994-10-03",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,1994-10-03,1998-10-05,Malmö kommuns valkrets,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1994-10-03",
                    end_date="1998-10-05",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,1998-10-05,2001-10-29,Malmö kommuns valkrets,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="1998-10-05",
                    end_date="2001-10-29",
                    start_flag="D",
                    end_flag="D",
                ),
                # Q5556026,2001-10-30,2002-09-30,Malmö kommuns valkrets,ledamot
                md.TermOfOffice(
                    office_type_id=1,
                    sub_office_type_id=0,
                    start_date="2001-10-30",
                    end_date="2002-09-30",
                    start_flag="D",
                    end_flag="D",
                ),
            ],
            key=lambda x: x.start_year,
        )  # type: ignore
    )

    assert len(person_index.property_values_specs) == 5


def test_overload_by_person():
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    assert database is not None

    person_index: md.PersonIndex = md.PersonIndex(database).load()
    person_ids: list[str] = ['Q5715273', 'Q5556026', 'Q5983926', 'unknown']
    df: pd.DataFrame = pd.DataFrame(data=dict(person_id=person_ids))
    df_overloaded: pd.DataFrame = person_index.overload_by_person(df)
    assert df_overloaded is not None
    assert set(df_overloaded.columns) == {'person_id', 'pid', 'gender_id', 'party_id'}
    assert (df_overloaded.pid == [person_index.person_id2pid.get(x) for x in person_ids]).all()
    assert (df_overloaded.gender_id == [1, 1, 1, 0]).all()
    assert (df_overloaded.party_id == [9, 0, 7, 0]).all()

    df_decoded: pd.DataFrame = person_index.lookups.decode(df_overloaded)
    assert set(df_decoded.columns) == {'person_id', 'gender', 'party_abbrev'}


def test_swap_rows(person_index: md.PersonIndex):
    Q5556026_pid = index_of_person_id(person_index.persons, 'Q5556026')
    Q5556026_pid = index_of_person_id(person_index.persons, 'Q5556026')

    Q5556026_data = person_index.persons.iloc[Q5556026_pid].copy()
    Q5556026_data = person_index.persons.iloc[Q5556026_pid].copy()

    swap_rows(person_index.persons, Q5556026_pid, Q5556026_pid)

    assert (person_index.persons.iloc[Q5556026_pid].copy() == Q5556026_data).all()
    assert (person_index.persons.iloc[Q5556026_pid].copy() == Q5556026_data).all()


def test_person_party_at():
    person: md.Person = dummy()
    person.alt_parties = [
        md.PersonParty(party_id=8, start_date=1950, end_date=1952),
        md.PersonParty(party_id=1, start_date=1952, end_date=1953),
        md.PersonParty(party_id=10, start_date=1955, end_date=1956),
    ]
    person.party_id = 5
    assert person.party_at(1950) == 5
    assert person.party_at(1952) == 5
    assert person.party_at(1953) == 5
    assert person.party_at(1954) == 5
    assert person.party_at(1955) == 5
    assert person.party_at(1956) == 5

    person.party_id = 0
    assert person.party_at(1950) == 8
    assert person.party_at(1952) == 8  # could be 1 if we consder dates
    assert person.party_at(1953) == 1
    assert person.party_at(1954) == 0
    assert person.party_at(1955) == 10
    assert person.party_at(1956) == 10

    """Ambigoues parties"""
    person.alt_parties = [
        md.PersonParty(party_id=8, start_date=1923, end_date=1926),
        md.PersonParty(party_id=1, start_date=0, end_date=0),
        md.PersonParty(party_id=10, start_date=0, end_date=0),
    ]

    assert person.party_at(1923) == 8
    assert person.party_at(1922) == 1
    assert person.party_at(1990) == 1


def test_speaker_info_service(person_index: md.PersonIndex):
    database: str = ConfigStore.config().get("metadata.database.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)

    person: md.Person = service.person_index.get_person('Q5556026')
    assert person.alt_parties
    assert len(person.alt_parties) == 9
    assert set(a.party_id for a in person.alt_parties) == {1, 7, 10}
    assert set(a.start_year for a in person.alt_parties) == {1985, 1988, 1991, 1994, 1998, 1983, 2001, 2002}
    assert set(a.end_year for a in person.alt_parties) == {9999, 1985, 1988, 1991, 1994, 1998, 2001, 2002}
    assert person.party_at(1950) == 0
    assert person.party_at(1994) == 7
    assert person.party_at(2000) == 7
    assert person.party_at(2010) == 10


@pytest.mark.skip("No unknown in test data")
def test_unknown(person_index: md.PersonIndex):
    database: str = ConfigStore.config().get("metadata.database.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)
    person: md.Person = service.person_index.get_person('unknown')

    assert person
    assert person.pid == 0
    assert person.party_id == 0
    assert person.gender_id == 0

    assert service.utterance_index

    u_id: str = 'i-b5b6a1f0ed7099a3-4'

    assert service.utterance_index.unknown_gender_lookup.get(u_id) == 1

    speaker: md.SpeakerInfo = service.get_speaker_info(u_id=u_id)
    assert speaker.person_id == "unknown"
    assert speaker.gender_id == 1
    assert speaker.party_id == 8

    speaker: md.SpeakerInfo = service.get_speaker_info(u_id='i-957dd4ed552513d0-37')
    assert speaker.person_id == "unknown"
    assert speaker.gender_id == 1
    assert speaker.party_id == 8
