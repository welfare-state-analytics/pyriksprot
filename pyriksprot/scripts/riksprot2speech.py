import click
from tqdm import tqdm

from pyriksprot import corpus_index, dispatch, interface
from pyriksprot.tagged_corpus import iterate

CONTENT_TYPES = [e.value for e in interface.ContentType]
TARGET_TYPES = [e.value for e in dispatch.TargetType]


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--target-type', default='zip', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option(
    '--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Content type to extract'
)
def main(
    source_folder: str = None, target_name: str = None, target_type: str = None, content_type: str = 'tagged_frame'
):
    content_type: interface.ContentType = interface.ContentType(content_type)
    target_type: dispatch.TargetType = dispatch.TargetType(target_type)
    segment_level: interface.SegmentLevel = interface.SegmentLevel.Speech

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
    )

    with dispatch.IDispatcher.get_cls(target_type)(
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
    ) as dispatcher:
        for segment in tqdm(segments):
            dispatcher.dispatch([segment])


if __name__ == "__main__":
    main()
