from os.path import basename

from pyriksprot import interface, to_speech, workflows
from pyriksprot.dispatch import dispatch

# pylint: disable=redefined-outer-name

TAG = "vx.y.z"


def main(target_type: dispatch.TargetTypeKey, source_folder: str):
    workflows.extract_corpus_tags(
        source_folder=source_folder,
        metadata_filename=f"/data/westac/riksdagen_corpus_data/metadata/riksprot_metadata.{TAG}.db",
        target_name=f'/data/westac/riksdagen_corpus_data/{target_type}_{basename(source_folder)}/',
        target_type=target_type,
        content_type=interface.ContentType.TaggedFrame,
        compress_type=dispatch.CompressType.Feather,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=to_speech.MergeStrategyType.chain,
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
    target_type: dispatch.TargetTypeKey = 'single-id-tagged-frame'
    source_folder: str = '/data/riksdagen_corpus_data/tagged_frames_{TAG}_profile'
    main(target_type, source_folder=source_folder)
