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
        person_id='i-cNo6XnCMDc2LkrvXc6U2b',
        wiki_id='Q5719585',
        name='Gustaf von Essen',
        gender_id=1,
        party_id=0,
        year_of_birth=1943,
        year_of_death=None,
        terms_of_office=[
            md.TermOfOffice(office_type_id=1, sub_office_type_id=0, start_date="1991-09-30", end_date="1994-10-03"),
            md.TermOfOffice(office_type_id=1, sub_office_type_id=0, start_date="1994-10-03", end_date="1998-10-05"),
            md.TermOfOffice(office_type_id=1, sub_office_type_id=0, start_date="1998-10-05", end_date="2002-09-30"),
        ],
        alt_parties=[
            md.PersonParty(party_id=7, start_date="1991-09-30", end_date="1994-10-03"),
            md.PersonParty(party_id=7, start_date="1994-10-03", end_date="1998-10-05"),
            md.PersonParty(party_id=7, start_date="1998-10-05", end_date="2002-09-30"),
            md.PersonParty(party_id=3, start_date=0, end_date=0),
            md.PersonParty(party_id=7, start_date=0, end_date=0),
        ],
    )


def test_code_lookups():
    database: str = ConfigStore.config().get("metadata.database.options.filename")

    assert database is not None

    lookups: md.Codecs = md.Codecs().load(database)
    assert lookups

    assert lookups.gender2id.get("Kvinna") == 2
    assert lookups.gender2name.get(2) == "Kvinna"

    assert lookups.gender2abbrev.get(2) == "K"
    assert lookups.gender2abbrev.get(0) == "?"

    assert lookups.decode_any_id("gender_id", 2) == "Kvinna"
    assert lookups.decode_any_id("gender_id", 2, to_name="gender_abbrev") == "K"

    assert lookups.office_type2id.get("unknown") == 0
    assert lookups.office_type2id.get("Minister") == 2
    assert lookups.office_type2name.get(2) == "Minister"

    assert lookups.sub_office_type2name.get(0) == "unknown"
    assert lookups.sub_office_type2name.get(1) == "Ledamot av första kammaren"
    assert lookups.sub_office_type2id.get("Ledamot av första kammaren") == 1

    assert lookups.party_abbrev2name.get(0) == "[-]"

    assert lookups.protocol_name2chamber_abbrev.get('prot-1933--fk--005') == 'fk'

    df: pd.DataFrame = pd.DataFrame(
        data=dict(
            gender_id=[
                lookups.gender2id.get('Okänt'),
                lookups.gender2id.get('Man'),
                lookups.gender2id.get('Kvinna'),
            ],
            party_id=[
                lookups.party_abbrev2id.get('[-]'),
                lookups.party_abbrev2id.get('S'),
                lookups.party_abbrev2id.get('L'),
            ],
            office_type_id=[0, 2, 3],
            sub_office_type_id=[
                0,
                lookups.sub_office_type2id.get('finansminister'),
                lookups.sub_office_type2id.get('andra kammarens andre vice talman'),
            ],
        )
    )
    id_columns: set[str] = {'gender_id', 'party_id', 'office_type_id', 'sub_office_type_id'}
    name_columns: set[str] = {'gender', 'gender_abbrev', 'party_abbrev', 'office_type', 'sub_office_type'}
    df_decoded: pd.DataFrame = lookups.decode(df, drop=False)

    assert set(df_decoded.columns) == id_columns.union(name_columns)
    assert (df_decoded.party_abbrev == ['[-]', 'S', 'L']).all()
    assert (df_decoded.gender == ['Okänt', 'Man', 'Kvinna']).all()
    assert (df_decoded.office_type == ['unknown', 'Minister', 'Talman']).all()
    assert (df_decoded.sub_office_type == ['unknown', 'finansminister', 'andra kammarens andre vice talman']).all()

    df_decoded: pd.DataFrame = lookups.decode(df, drop=True)
    assert set(df_decoded.columns) == name_columns

    assert lookups.property_values_specs and len(lookups.property_values_specs) == 4


def test_person_index(person_index: md.PersonIndex):
    wiki_id: str = 'Q5556026'
    person_id: str = 'i-84xpErSjuTzEi5hvTA1Jtt'
    pid: int = person_index.wiki_id2pid.get(wiki_id)
    expected_data: dict = {
        'person_id': person_id,
        'wiki_id': wiki_id,
        'name': 'Sten Andersson',
        'gender_id': 1,
        'party_id': 0,
        'year_of_birth': 1943,
        'year_of_death': 2010,
    }

    person: md.Person = person_index.get_person(wiki_id)
    assert expected_data == {k: v for k, v in asdict(person).items() if k in expected_data}

    person: md.Person = person_index.get_person(pid)
    assert expected_data == {k: v for k, v in asdict(person).items() if k in expected_data}

    person_lookup: dict[str, md.Person] = person_index.person_lookup
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
    wiki_ids: list[str] = ['Q5715273', 'Q5556026', 'Q5983926', 'unknown']
    person_ids: list[str] = [person_index.wiki_id2person_id.get(x) for x in wiki_ids]

    df: pd.DataFrame = pd.DataFrame(data=dict(person_id=person_ids))
    df_overloaded: pd.DataFrame = person_index.overload_by_person(df)

    assert df_overloaded is not None
    assert set(df_overloaded.columns) == {'person_id', 'pid', 'gender_id', 'party_id'}
    assert (df_overloaded.pid == [person_index.person_id2pid.get(x) for x in person_ids]).all()
    assert (df_overloaded.gender_id == [1, 1, 1, 0]).all()

    px = person_index.lookups.party_abbrev2id.get
    assert (df_overloaded.party_id == [px('S'), 0, px('M'), 0]).all()

    df_decoded: pd.DataFrame = person_index.lookups.decode(df_overloaded)
    assert set(df_decoded.columns) == {'person_id', 'gender', 'gender_abbrev', 'party_abbrev'}


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


def test_id_mappings(person_index: md.PersonIndex):

    database: str = ConfigStore.config().get("metadata.database.options.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)

    wiki_id: str = 'Q5556026'
    person_id: str = person_index.wiki_id2person_id.get(wiki_id)

    assert person_id == 'i-84xpErSjuTzEi5hvTA1Jtt'

    person_by_wiki_id: md.Person = service.person_index.get_person(wiki_id)
    person_by_person_id: md.Person = service.person_index.get_person(person_id)
    person_by_pid: md.Person = service.person_index.get_person(person_by_wiki_id.pid)

    assert person_by_person_id.wiki_id == person_by_wiki_id.wiki_id
    assert person_by_person_id.person_id == person_by_wiki_id.person_id
    assert person_by_person_id.pid == person_by_wiki_id.pid
    assert person_by_pid.wiki_id == person_by_wiki_id.wiki_id
    assert person_by_pid.person_id == person_by_wiki_id.person_id


def test_get_person_by_ids(person_index: md.PersonIndex):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)

    wiki_id: str = 'Q5556026'
    person_id: str = person_index.wiki_id2person_id.get(wiki_id)

    assert person_id == 'i-84xpErSjuTzEi5hvTA1Jtt'

    person_by_wiki_id: md.Person = service.person_index.get_person(wiki_id)
    person_by_person_id: md.Person = service.person_index.get_person(person_id)
    person_by_pid: md.Person = service.person_index.get_person(person_by_wiki_id.pid)

    assert person_by_person_id.wiki_id == person_by_wiki_id.wiki_id
    assert person_by_person_id.person_id == person_by_wiki_id.person_id
    assert person_by_person_id.pid == person_by_wiki_id.pid
    assert person_by_pid.wiki_id == person_by_wiki_id.wiki_id
    assert person_by_pid.person_id == person_by_wiki_id.person_id


def test_speaker_info_service(person_index: md.PersonIndex):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)

    wiki_id: str = 'Q5556026'

    person: md.Person = service.person_index.get_person(wiki_id)

    assert person.alt_parties
    assert len(person.alt_parties) == 9
    assert set(a.start_year for a in person.alt_parties) == {1985, 1988, 1991, 1994, 1998, 1983, 2001, 2002}
    assert set(a.end_year for a in person.alt_parties) == {9999, 1985, 1988, 1991, 1994, 1998, 2001, 2002}
    assert set(a.party_id for a in person.alt_parties) == {7, 5, 47}
    assert person.party_at(1950) == 0
    assert person.party_at(1994) == 5
    assert person.party_at(2000) == 5
    assert person.party_at(2010) == 7


@pytest.mark.skip("No unknown in test data")
def test_unknown(person_index: md.PersonIndex):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    service = md.SpeakerInfoService(database, person_index=person_index)
    person: md.Person = service.person_index.get_person('unknown')

    assert person
    assert person.pid == 0
    assert person.party_id == 0
    assert person.gender_id == 0

    assert service.utterance_index

    u_id: str = 'i-b5b6a1f0ed7099a3-4'

    # assert service.utterance_index.unknown_gender_lookup.get(u_id) == 1

    speaker: md.SpeakerInfo = service.get_speaker_info(u_id=u_id)
    assert speaker.person_id == "unknown"
    assert speaker.gender_id == 1
    assert speaker.party_id == 8

    speaker: md.SpeakerInfo = service.get_speaker_info(u_id='i-957dd4ed552513d0-37')
    assert speaker.person_id == "unknown"
    assert speaker.gender_id == 1
    assert speaker.party_id == 8
