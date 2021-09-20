import pyriksprot


def main():
    opts = {
        'source_folder': '/data/riksdagen_corpus_data/riksdagen-corpus/corpus',
        'target': '.',
        'level': 'speaker',
        'dedent': False,
        'dehyphen': False,
        'keep_order': False,
        'skip_size': 1,
        'processes': None,
        'years': '1920',
        'temporal_key': 'year',
        'group_keys': ('party', 'gender', 'who'),
        'create_index': False,
        '_': {},
    }

    pyriksprot.extract_corpus_text(**opts)


if __name__ == '__main__':
    main()
