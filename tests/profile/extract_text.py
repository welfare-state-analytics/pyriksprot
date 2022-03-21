from pyriksprot import interface, workflows


def main():
    opts = {
        'source_folder': '/data/riksdagen_corpus_data/riksdagen-corpus/corpus',
        'metadata_filename': '/data/riksdagen_corpus_data/metadata/riksprot_metadata.db',
        'target_name': '.',
        'segment_level': interface.SegmentLevel.Who,
        'temporal_key': interface.TemporalKey.Year,
        'group_keys': (interface.GroupingKey.party_id, interface.GroupingKey.gender_id, interface.GroupingKey.who),
        'years': '1920',
        'segment_skip_size': 1,
        'multiproc_keep_order': False,
        'multiproc_processes': None,
        'create_index': False,
        'dedent': False,
        'dehyphen': False,
        '_': {},
    }

    workflows.extract_corpus_text(**opts)


if __name__ == '__main__':
    main()
