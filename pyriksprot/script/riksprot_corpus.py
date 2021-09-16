import sys

import click
from pyriksprot import convert_protocol
from pyriksprot.resources.templates import PARLA_TEMPLATES_SHORTNAMES

"""
Extract a corpus of text or annotations from 
"""
@click.command()
@click.argument('input_filename', type=click.STRING)
@click.option(
    '-o',
    '--output-filename',
    default=None,
    help='Output filename (default stdout)',
    type=click.STRING,
)
@click.option(
    '-t',
    '--template-name',
    default='speeches.xml',
    type=click.Choice(PARLA_TEMPLATES_SHORTNAMES),
    help='Template to use',
)
# @click.option('-dehyphen', '--dehyphen/--no-dehyphen', default=True, is_flag=True, help='Dehyphen text')
def main(
    input_filename: str = None,
    output_filename: str = None,
    template_name: str = None,
):
    try:

        convert_protocol(input_filename, output_filename, template_name)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
