import click
from tqdm import tqdm

from pyriksprot import corpus_index, dispatch, interface, segment
from pyriksprot.tagged_corpus import iterate

from .utils import option2


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@option2('--content-type')
@option2('--target-type')
@option2('--compress-type')
@option2('--merge-strategy')
def main(
    source_folder: str = None,
    target_name: str = None,
    content_type: str = 'tagged_frame',
    target_type: str = None,
    compress_type: str = "zip",
    merge_strategy: str = "chain",
):

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )
    segments: segment.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType(content_type),
        segment_level=interface.SegmentLevel.Protocol,
        speech_merge_strategy=segment.MergeSpeechStrategyType(merge_strategy),
    )

    with dispatch.IDispatcher.dispatcher(target_type)(
        target_name=target_name,
        compress_type=dispatch.CompressType(compress_type.lower()),
    ) as dispatcher:
        for s in tqdm(segments):
            dispatcher.dispatch([s])


if __name__ == "__main__":
    main()
