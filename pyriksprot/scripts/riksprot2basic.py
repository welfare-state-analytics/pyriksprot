import click
from tqdm import tqdm

from pyriksprot import corpus_index, dispatch, interface
from pyriksprot.tagged_corpus import iterate

TARGET_TYPES = dispatch.IDispatcher.dispatcher_keys()
COMPRESS_TYPES = dispatch.CompressType.values()
CONTENT_TYPES = [e.value for e in interface.ContentType]


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Text or tags')
@click.option('--target-type', default='checkpoint', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option('--compress-type', default='zip', type=click.Choice(COMPRESS_TYPES), help='Compress type')
def main(
    source_folder: str = None,
    target_name: str = None,
    content_type: str = 'tagged_frame',
    target_type: str = None,
    compress_type: str = "zip",
):

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType(content_type),
        segment_level=interface.SegmentLevel.Protocol,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
    )

    with dispatch.IDispatcher.dispatcher(target_type)(
        target_name=target_name,
        compress_type=dispatch.CompressType(compress_type.lower()),
    ) as dispatcher:
        for segment in tqdm(segments):
            dispatcher.dispatch([segment])


if __name__ == "__main__":
    main()
