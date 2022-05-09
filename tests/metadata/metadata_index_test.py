import sqlite3
from contextlib import closing
from dataclasses import asdict

import pandas as pd
import pytest

from pyriksprot import metadata as md
from pyriksprot.metadata.generate import EXTRA_TABLES, register_numpy_adapters, sql_ddl_create, sql_ddl_insert
from pyriksprot.metadata.person import index_of_person_id, swap_rows

from ..utility import SAMPLE_METADATA_DATABASE_NAME

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
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1922, end_year=1924),
            md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1929, end_year=1944),
        ],
        alt_parties=[
            md.PersonParty(party_id=8, start_year=0, end_year=0),
            md.PersonParty(party_id=1, start_year=0, end_year=0),
            md.PersonParty(party_id=10, start_year=0, end_year=0),
        ],
    )


def test_code_lookups():
    lookups: md.Codecs = md.Codecs().load(SAMPLE_METADATA_DATABASE_NAME)
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
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1983, end_year=1985),
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1985, end_year=1988),
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1988, end_year=1991),
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1991, end_year=1994),
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1994, end_year=1998),
                md.TermOfOffice(office_type_id=1, sub_office_type_id=3, start_year=1998, end_year=2002),
            ],
            key=lambda x: x.start_year,
        )
    )

    assert len(person_index.property_values_specs) == 5


def test_overload_by_person(person_index: md.PersonIndex):
    person_index: md.PersonIndex = md.PersonIndex(SAMPLE_METADATA_DATABASE_NAME).load()
    person_ids: list[str] = ['Q5715273', 'Q5556026', 'Q5983926', 'unknown']
    df: pd.DataFrame = pd.DataFrame(data=dict(person_id=person_ids))
    df_overloaded: pd.DataFrame = person_index.overload_by_person(df)
    assert df_overloaded is not None
    assert set(df_overloaded.columns) == {'person_id', 'pid', 'gender_id', 'party_id'}
    assert (df_overloaded.pid == [person_index.person_id2pid.get(x) for x in person_ids]).all()
    assert (df_overloaded.gender_id == [1, 1, 1, 0]).all()
    assert (df_overloaded.party_id == [8, 0, 6, 0]).all()

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
        md.PersonParty(party_id=8, start_year=1950, end_year=1952),
        md.PersonParty(party_id=1, start_year=1952, end_year=1953),
        md.PersonParty(party_id=10, start_year=1955, end_year=1956),
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
        md.PersonParty(party_id=8, start_year=1923, end_year=1926),
        md.PersonParty(party_id=1, start_year=0, end_year=0),
        md.PersonParty(party_id=10, start_year=0, end_year=0),
    ]

    assert person.party_at(1923) == 8
    assert person.party_at(1922) == 0
    assert person.party_at(1990) == 0


def test_speaker_info_service(person_index: md.PersonIndex):
    service = md.SpeakerInfoService(SAMPLE_METADATA_DATABASE_NAME, person_index=person_index)

    person: md.Person = service.person_index.get_person('Q5556026')
    assert person.alt_parties
    assert len(person.alt_parties) == 9
    assert set(a.party_id for a in person.alt_parties) == {1, 6}
    assert set(a.start_year for a in person.alt_parties) == {1985, 1988, 1991, 1994, 1998, 1983, 2001, 2002}
    assert set(a.end_year for a in person.alt_parties) == {0, 1985, 1988, 1991, 1994, 1998, 2001, 2002}
    assert person.party_at(1950) == 0
    assert person.party_at(1994) == 6
    assert person.party_at(2000) == 6
    assert person.party_at(2010) == 1


def test_unknown(person_index: md.PersonIndex):
    service = md.SpeakerInfoService(SAMPLE_METADATA_DATABASE_NAME, person_index=person_index)
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


@pytest.mark.skip("infra test")
def test_load_speaker_index():

    database_filename: str = "/data/riksdagen_corpus_data/metadata/riksprot_metadata.main.db"

    db = sqlite3.connect(database_filename)

    register_numpy_adapters()

    tablename: str = "speech_index"
    specification: dict[str, str] = EXTRA_TABLES[tablename]

    with closing(db.cursor()) as cursor:
        cursor.executescript(f"drop table if exists {tablename};")
        cursor.executescript(sql_ddl_create(tablename=tablename, specification=specification))

    speech_index: pd.DataFrame = pd.read_feather(
        "/data/riksdagen_corpus_data/tagged_frames_v0.4.2_speeches.feather/document_index.feather"
    )

    columns: list[str] = [k for k in specification if k[0] not in "+:"]
    data = speech_index[columns].to_records(index=False)

    with closing(db.cursor()) as cursor:
        insert_sql = sql_ddl_insert(tablename=tablename, columns=columns)
        cursor.executemany(insert_sql, data)


@pytest.mark.skip("infra test")
def test_load_speaker_index2():

    database_filename: str = "/data/riksdagen_corpus_data/metadata/riksprot_metadata.main.db"
    speech_index_filename: str = (
        "/data/riksdagen_corpus_data/tagged_frames_v0.4.2_speeches.feather/document_index.feather"
    )
    speech_index: pd.DataFrame = pd.read_feather(speech_index_filename)

    with sqlite3.connect(database_filename) as db:
        speech_index.to_sql("speech_index", db, if_exists="replace")
