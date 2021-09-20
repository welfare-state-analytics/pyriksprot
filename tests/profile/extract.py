import pyriksprot

def main():
    opts = {
        'source_folder': 'tests/test_data/source',
        'target': 'tests/output/',
        'level': 'speaker',
        'dedent': False,
        'dehyphen': False,
        'keep_order': False,
        'skip_size': 1,
        'processes': None,
        'years': None,
        'temporal_key': 'year',
        'group_keys': ('party', ),
        'create_index': False,
        '_': {},
    }

    pyriksprot.extract_corpus_text(**opts)

if __name__ == '__main__':
    main()

