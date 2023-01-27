def input_unknown_url(tag: str = "main"):
    return (
        f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/input/matching/unknowns.csv"
    )


def table_url(tablename: str, tag: str = "main") -> str:
    if tablename == "unknowns":
        return input_unknown_url(tag)
    return f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/metadata/{tablename}.csv"


RIKSPROT_METADATA_TABLES: dict = {
    'government': {
        'government_id': 'text not null', # actual primary key
        'government': 'text primary key not null',
        'start': 'date',
        'end': 'date',
        # ':options:': {'auto_increment': 'government_id'},
        ':index:': {},
        ':drop_duplicates:': 'government',
    },
    'location_specifier': {
        # 'location_specifier_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'location': 'text',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'member_of_parliament': {
        # 'member_of_parliament_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'district': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'minister': {
        'person_id': 'text references person (person_id) not null',
        'government': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'name': {
        'person_id': 'text references person (person_id) not null',
        'name': 'text not null',
        'primary_name': 'integer not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'party_abbreviation': {
        'party': 'text primary key not null',
        'abbreviation': 'text not null',
        'ocr_correction': 'text',
    },
    'party_affiliation': {
        'person_id': 'text references person (person_id) not null',
        'start': 'int',
        'end': 'int',
        'party': 'text',
        'party_id': 'text',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'person': {
        'person_id': 'text primary key',
        'born': 'int',
        'dead': 'int',
        'gender': 'text',
        'wiki_id': 'text',
        'riksdagen_id': 'text',
        ':drop_duplicates:': 'wiki_id',
        ':copy_column:': {'person_id': 'wiki_id'},
    },
    'speaker': {
        'person_id': 'text references person (person_id) not null',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'twitter': {
        'twitter': 'text',  # primary key',
        'person_id': 'text references person (person_id) not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'alias': {
        # 'alias_id': 'AUTO_INCREMENT'
        'person_id': 'text references person (person_id) not null',  # compound key
        'alias': 'text not null',  # compound key
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'name_location_specifier': {
        # 'name_location_specifier_id': 'AUTO_INCREMENT'
        'person_id': 'text references person (person_id) not null',  # compound key
        'alias': 'text',  # compound key
        'name': 'text not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'unknowns': {
        'protocol_id': 'text',  # primary key',
        'uuid': 'text',
        'gender': 'text',
        'party': 'text',
        'other': 'text',
        ':url:': input_unknown_url,
    },
}

EXTRA_TABLES = {
    'speech_index': {
        'document_id': 'int primary key',
        'document_name': 'text',
        'year': 'int',
        'who': 'text',
        'gender_id': 'int',
        'party_id': 'int',
        'office_type_id': 'int',
        'sub_office_type_id': 'int',
        'n_tokens': 'int',
        'filename': 'text',
        'u_id': 'text',
        'n_utterances': 'int',
        'speaker_note_id': 'text',
        'speach_index': 'int',
    },
}

PERSON_TABLES: list[str] = [key for key, item in RIKSPROT_METADATA_TABLES.items() if 'person_id' in item]
