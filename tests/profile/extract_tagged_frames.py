from os.path import basename

from pyriksprot import dispatch, interface, tagged_corpus

# pylint: disable=redefined-outer-name


def main(target_type: dispatch.TargetTypeKey, source_folder: str):

    tagged_corpus.extract_corpus_tags(
        source_folder=source_folder,
        target_name=f'/data/westac/riksdagen_corpus_data/{target_type}_{basename(source_folder)}/',
        target_type=target_type,
        content_type=interface.ContentType.TaggedFrame,
        compress_type=dispatch.CompressType.Feather,
        segment_level=interface.SegmentLevel.Speech,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        temporal_key=None,
        group_keys=None,
        years=None,
        segment_skip_size=1,
        multiproc_keep_order=False,
        multiproc_processes=3,
        multiproc_chunksize=100,
        force=True,
        progress=True,
    )


if __name__ == '__main__':
    # target_type: dispatch.TargetTypeKey = 'single-id-tagged-frame-per-group'
    target_type: dispatch.TargetTypeKey = 'single-id-tagged-frame'
    source_folder: str = '/data/westac/riksdagen_corpus_data/tagged_frames_v0.3.0_20201218'
    # source_folder: str = './data/tagged_protocols_1965'
    main(target_type, source_folder=source_folder)
