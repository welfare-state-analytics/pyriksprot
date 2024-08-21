from pyriksprot.metadata.config import input_unknown_url
from pyriksprot.metadata.utility import fix_ts_config

TABLE_DEFINITIONS: dict = {
    'alias': {
        # 'alias_id': 'AUTO_INCREMENT'
        'person_id': 'text references person (person_id) not null',  # compound key
        'alias': 'text not null',  # compound key
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'government': {
        'government_id': 'text not null',  # actual primary key
        'government': 'text primary key not null',
        'start': 'text',
        'end': 'text',
        # ':options:': {'auto_increment': 'government_id'},
        ':index:': {},
        ':drop_duplicates:': 'government',
        ':compute:': [
            fix_ts_config("start", "truncate"),
            fix_ts_config("end", "extend"),
        ],
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
        ':compute:': [
            fix_ts_config("start", "truncate"),
            fix_ts_config("end", "extend"),
        ],
    },
    'minister': {
        'person_id': 'text references person (person_id) not null',
        'government': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            fix_ts_config("start", "truncate"),
            fix_ts_config("end", "extend"),
        ],
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
        ':compute:': [
            fix_ts_config("start", "truncate"),
            fix_ts_config("end", "extend"),
        ],
    },
    'person': {
        'person_id': 'text primary key',
        'born': 'date',
        'dead': 'date',
        'gender': 'text',
        'wiki_id': 'text',
        'riksdagen_id': 'text',
        ':drop_duplicates:': 'wiki_id',
        ':copy_column:': {'person_id': 'wiki_id'},
        ':compute:': [
            fix_ts_config("born", "truncate"),
            fix_ts_config("dead", "extend"),
        ],
    },
    'speaker': {
        'person_id': 'text references person (person_id) not null',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            fix_ts_config("start", "truncate"),
            fix_ts_config("end", "extend"),
        ],
    },
    'twitter': {
        'twitter': 'text',  # primary key',
        'person_id': 'text references person (person_id) not null',
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
        ':is_extra:': True,
    },
}
