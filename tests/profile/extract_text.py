from pyriksprot import interface, parlaclarin


def main():
    opts = {
        'source_folder': '/data/westac/riksdagen_corpus_data/riksdagen-corpus/corpus',
        'metadata_filename': '/data/westac/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.0.db',
        'target_name': '.',
        'segment_level': interface.SegmentLevel.Who,
        'temporal_key': interface.TemporalKey.Year,
        'group_keys': (interface.GroupingKey.Party, interface.GroupingKey.Gender, interface.GroupingKey.Who),
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
