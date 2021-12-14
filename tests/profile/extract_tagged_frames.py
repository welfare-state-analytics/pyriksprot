from pyriksprot import dispatch, interface, tagged_corpus


def main():

    target_type: dispatch.TargetTypeKey = 'single-id-tagged-frame-per-group'
    tagged_corpus.extract_corpus_tags(
        source_folder='/data/riksdagen_corpus_data/tagged_frames_1961/',
        target_name='./data/1961.id.feather/',
        target_type=target_type,
        content_type=interface.ContentType.TaggedFrame,
        compress_type=dispatch.CompressType.Feather,
        segment_level=interface.SegmentLevel.Speech,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        temporal_key=None,
        group_keys=['who'],
        years=None,
        segment_skip_size=1,
        multiproc_keep_order=False,
        multiproc_processes=None,
        multiproc_chunksize=100,
        force=True,
    )


if __name__ == '__main__':
    main()
