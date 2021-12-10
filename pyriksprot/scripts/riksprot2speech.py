import zipfile

import click

from pyriksprot import dispatch, interface
from pyriksprot.tagged_corpus import extract

CONTENT_TYPES = [e.value for e in interface.ContentType]
TARGET_TYPES = [e.value for e in dispatch.TargetType]
COMPRESSION_TYPES = {
    "DEFLATED": zipfile.ZIP_DEFLATED,
    "STORED": zipfile.ZIP_STORED,
    "LZMA": zipfile.ZIP_LZMA,
}


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--target-type', default='checkpoint', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option(
    '--compression-type',
    default='DEFLATED',
    type=click.Choice(list(COMPRESSION_TYPES.keys())),
    help='Target compression type',
)
@click.option(
    '--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Content type to extract'
)
@click.option('--processes', default=1, type=click.INT, help='Number of processes')
def main(
    source_folder: str = None,
    target_name: str = None,
    target_type: str = None,
    content_type: str = 'tagged_frame',
    compression_type: str = 'DEFLATED',
    processes: int = 1,
):
    content_type: interface.ContentType = interface.ContentType(content_type.lower())
    target_type: dispatch.TargetType = dispatch.TargetType(target_type.lower())
    compression: int = COMPRESSION_TYPES[compression_type.upper()]

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
        multiproc_keep_order=False,
        multiproc_processes=processes,
        multiproc_chunksize=10,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        compression=compression,
    )


if __name__ == "__main__":

    if True:  # pylint: disable=using-constant-test

        main()

    else:
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                '/data/riksdagen_corpus_data/tagged_frames',
                '/data/riksdagen_corpus_data/tagged-speech-corpus.feather',
                '--target-type',
                'feather',
                # '--compression-type',
                # 'LZMA',
                '--processes',
                1,
            ],
        )
        print(result.output)
