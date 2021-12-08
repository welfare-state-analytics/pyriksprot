import click

from pyriksprot import dispatch, interface
from pyriksprot.tagged_corpus import extract

CONTENT_TYPES = [e.value for e in interface.ContentType]
TARGET_TYPES = [e.value for e in dispatch.TargetType]


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--target-type', default='checkpoint', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option(
    '--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Content type to extract'
)
def main(
    source_folder: str = None, target_name: str = None, target_type: str = None, content_type: str = 'tagged_frame'
):
    content_type: interface.ContentType = interface.ContentType(content_type)
    target_type: dispatch.TargetType = dispatch.TargetType(target_type)

    extract.extract_corpus_tags(
        source_folder=source_folder,
        target_name=target_name,
        content_type=content_type,
        target_type=target_type,
        segment_level=interface.SegmentLevel.Speech,
        segment_skip_size=1,
        years=None,
        temporal_key=None,
        group_keys=None,
        multiproc_keep_order=None,
        multiproc_processes=1,
        multiproc_chunksize=100,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
    )


if __name__ == "__main__":
    # main()

    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(
        main, ['/data/riksdagen_corpus_data/tagged_frames/', '/data/riksdagen_corpus_data/tagged-speech-corpus']
    )
    print(result.output)
