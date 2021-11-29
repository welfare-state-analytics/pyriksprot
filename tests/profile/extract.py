from pyriksprot import parlaclarin


def main():
    opts = {
        'source_folder': '/data/riksdagen_corpus_data/riksdagen-corpus/corpus',
        'target_name': '.',
        'segment_level': 'who',
        'temporal_key': 'year',
        'group_keys': ('party', 'gender', 'who'),
        'years': '1920',
        'segment_skip_size': 1,
        'multiproc_keep_order': False,
        'multiproc_processes': None,
        'create_index': False,
        'dedent': False,
        'dehyphen': False,
        '_': {},
    }

    parlaclarin.extract_corpus_text(**opts)


if __name__ == '__main__':
    main()
