import sys
from inspect import currentframe, getargvalues
from typing import Sequence

import click

import pyriksprot

sys.path.append(".")


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract an aggregated subset aof ParlaClarin corpus.
"""
LEVELS = ['protocol', 'speaker', 'speech', 'utterance', 'paragraph']
MODES = ['plain', 'zip', 'gzip', 'bz2', 'lzma']


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target', type=click.STRING)
@click.option('-m', '--mode', default='zipfile', type=click.Choice(MODES), help='Target type')
@click.option('-t', '--temporal-key', default=None, help='Temporal partition key(s)', type=click.STRING)
@click.option('-y', '--years', default=None, help='Years to include in output', type=click.STRING)
@click.option('-g', '--group-key', help='Partition key(s)', multiple=True, type=click.STRING)
@click.option('-p', '--processes', default=None, type=click.IntRange(1, 40), help='Number of processes to use')
@click.option('-l', '--level', default='speaker', type=click.Choice(LEVELS), help='Protocol extract level')
@click.option(
    '-e', '--keep-order', default=False, is_flag=True, help='Keep output in filename order (slower, multiproc)'
)
@click.option('-s', '--skip-size', default=1, type=click.IntRange(1, 1024), help='Skip blocks of char length less than')
@click.option('-d', '--dedent', default=False, is_flag=True, help='Remove indentation')
@click.option('-k', '--dehyphen', default=False, is_flag=True, help='Dehyphen text')
# @click.option('-x', '--create-index', default=False, is_flag=True, help='Create document index')
def main(
    source_folder: str = None,
    target: str = None,
    mode: str = None,
    years: str = None,
    temporal_key: str = None,
    level: str = None,
    dedent: bool = False,
    dehyphen: bool = False,
    keep_order: str = None,
    skip_size: int = 1,
    processes: int = 1,
    # create_index: bool = True,
    group_key: Sequence[str] = None,
):
    try:

        # kwargs = getargvalues(currentframe().f_back).locals['kwargs']

        # for k,v in kwargs.items():
        #     logger.info(f"{k}: {v}")

        pyriksprot.extract_corpus_text(
            source_folder=source_folder,
            target=target,
            target_mode=mode,
            level=level,
            dedent=dedent,
            dehyphen=dehyphen,
            keep_order=keep_order,
            skip_size=skip_size,
            processes=processes,
            years=years,
            temporal_key=temporal_key,
            group_keys=group_key,
            # create_index=create_index,
        )

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
