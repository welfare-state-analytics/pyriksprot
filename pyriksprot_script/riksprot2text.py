import sys
sys.path.append(".")
from inspect import currentframe, getargvalues
from typing import Sequence

import click

import pyriksprot


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract a aggregate and/or subsetted corpus of either text or tagged text.
"""
LEVELS = ['protocol', 'speaker', 'speech', 'utterance', 'paragraph']

@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target', type=click.STRING)
@click.option('-g', '--groupby', default=None, help='Partition key(s)', multiple=True, type=click.STRING)
@click.option('-p', '--processes', default=4, type=click.IntRange(1, 40), help='Number of processes to use')
@click.option(
    '-l',
    '--level',
    default='speaker',
    type=click.Choice(LEVELS),
    help='Protocol extract level',
)
@click.option(
    '--keep-order', default=False, is_flag=True, help='Keep output in filename order (slower, multiproc)'
)
@click.option('-s', '--skip-size', default=1, type=click.IntRange(1, 1024), help='Skip blocks of char length less than')
@click.option('-d', '--dedent', default=False, is_flag=True, help='Remove indentation')
@click.option('-k', '--dehyphen', default=False, is_flag=True, help='Dehyphen text')
@click.option('-x', '--create-index', default=False, is_flag=True, help='Create document index')

# @click.option('-c', '--concept', default=None, help='Concept', multiple=True, type=click.STRING)
# @click.option('-cp', '--compute-processes', default=None, help='Number of compute processes', type=click.INT)
# @click.option('-cc', '--compute-chunksize', default=10, help='Compute process chunksize', type=click.INT)
# @click.option('-i', '--pos-includes', default='', help='POS tags to include e.g. "|NN|JJ|".', type=click.STRING)
# @click.option('-m', '--pos-paddings', default='', help='POS tags to replace with a padding marker.', type=click.STRING)
# @click.option(
#     '-x',
#     '--pos-excludes',
#     default='',
#     help='List of POS tags to exclude e.g. "|MAD|MID|PAD|".',
#     type=click.STRING,
# )
# @click.option('-m', '--phrase', default=None, help='Phrase', multiple=True, type=click.STRING)
# @click.option('-z', '--phrase-file', default=None, help='Phrase filename', multiple=False, type=click.STRING)
# @click.option('-b', '--lemmatize/--no-lemmatize', default=True, is_flag=True, help='Use word baseforms')
# @click.option('-l', '--to-lower/--no-to-lower', default=True, is_flag=True, help='Lowercase words')
# @click.option(
#     '-r',
#     '--remove-stopwords',
#     default=None,
#     type=click.Choice(['swedish', 'english']),
#     help='Remove stopwords using given language',
# )
# @click.option(
#     '--tf-threshold',
#     default=1,
#     type=click.IntRange(1, 99),
#     help='Globoal TF threshold filter (words below filtered out)',
# )
# @click.option(
#     '--tf-threshold-mask',
#     default=False,
#     is_flag=True,
#     help='If true, then low TF words are kept, but masked as "__low_tf__"',
# )
# @click.option('--min-word-length', default=1, type=click.IntRange(1, 99), help='Min length of words to keep')
# @click.option('--max-word-length', default=None, type=click.IntRange(10, 99), help='Max length of words to keep')
# @click.option('--doc-chunk-size', default=None, help='Split document in chunks of chunk-size words.', type=click.INT)
# @click.option('--keep-symbols/--no-keep-symbols', default=True, is_flag=True, help='Keep symbols')
# @click.option('--keep-numerals/--no-keep-numerals', default=True, is_flag=True, help='Keep numerals')
# @click.option(
#     '--only-alphabetic', default=False, is_flag=True, help='Keep only tokens having only alphabetic characters'
# )
def main(
    source_folder: str = None,
    target: str = None,
    level: str = None,
    dedent: bool = False,
    dehyphen: bool = False,
    keep_order: str = None,
    skip_size: int = 1,
    processes: int = 1,
    create_index: bool = True,
    groupby: Sequence[str] = None,
):
    try:

        # kwargs = getargvalues(currentframe().f_back).locals['kwargs']

        # for k,v in kwargs.items():
        #     logger.info(f"{k}: {v}")

        pyriksprot.extract_corpus_text(
            source_folder=source_folder,
            target=target,
            level=level,
            dedent=dedent,
            dehyphen=dehyphen,
            keep_order=keep_order,
            skip_size=skip_size,
            processes=processes,
            create_index=create_index,
            groupby=groupby,
        )

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
