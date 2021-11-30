import click
from tqdm import tqdm

from pyriksprot import corpus_index, dispatch, interface, member, merge
from pyriksprot.tagged_corpus import iterate

CONTENT_TYPES = [e.value for e in interface.ContentType]


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--members-filename', default=None, help='Member index filename', type=click.STRING)
@click.option(
    '--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Content type to extract'
)
def main(
    source_folder: str = None, target_name: str = None, members_filename: str = None, content_type: str = 'tagged_frame'
):

    members_filename = members_filename or f'{source_folder}/members_of_parliament.csv'
    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )
    member_index: member.ParliamentaryMemberIndex = member.ParliamentaryMemberIndex(members_filename)
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType(content_type),
        segment_level=interface.SegmentLevel.Protocol,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
    )
    groups = merge.SegmentMerger(
        source_index=source_index, member_index=member_index, temporal_key=None, grouping_keys=None
    ).merge(segments)

    with dispatch.ZipFileDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
    ) as dispatcher:
        for group in tqdm(groups):
            dispatcher.dispatch(group)


if __name__ == "__main__":
    main()
