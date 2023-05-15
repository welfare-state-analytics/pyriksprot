"""Convert ParlaClarin XML protocol to other text format using Jinja."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, List, Union

from click import echo
from jinja2 import Environment, PackageLoader, Template, select_autoescape

from pyriksprot import to_speech
from pyriksprot.preprocess import dedent, dehyphen
from pyriksprot.utility import strip_paths

from . import parse

if TYPE_CHECKING:
    from .. import interface


JINJA_ENV = Environment(
    loader=PackageLoader('pyriksprot.corpus.parlaclarin.resources', 'templates'),
    autoescape=select_autoescape(['html', 'xml']),
    trim_blocks=True,
    lstrip_blocks=True,
)
JINJA_ENV.filters['dedent'] = dedent
JINJA_ENV.filters['dehyphen'] = dehyphen


class ProtocolConverter:
    """Transform ParlaClarin XML to template-based format."""

    def __init__(self, template: Union[str, Template]):
        """[summary]

        Args:
            template (Union[str, Template]): Jinja template.
        """

        if not template.endswith(".jinja"):
            template += ".jinja"

        if isinstance(template, str):
            template = JINJA_ENV.get_template(template)

        self.template: Template = template

    def convert(self, protocol: interface.Protocol, speeches: List[interface.Speech], filename: str) -> str:
        """Transform `protocol` and return resulting text."""
        text: str = self.template.render(protocol=protocol, speeches=speeches, filename=filename)
        return text


def convert_protocol(
    input_filename: str = None,
    output_filename: str = None,
    template_name: str = None,
    merge_strategy: to_speech.MergeStrategyType = to_speech.MergeStrategyType.who_speaker_note_id_sequence,
):
    """Convert protocol in `input_filename' using template `template_name`. Store result in `output_filename`.

    Args:
        input_filename (str, optional): Source file. Defaults to None.
        output_filename (str, optional): Target file. Defaults to None.
        template_name (str, optional): Template name (found in resource-folder). Defaults to None.
    """
    protocol: interface.Protocol = parse.ProtocolMapper.parse(input_filename)
    content: str = ""

    if protocol.has_text:
        converter: ProtocolConverter = ProtocolConverter(template_name)
        speeches: List[interface.Speech] = to_speech.to_speeches(protocol=protocol, merge_strategy=merge_strategy)
        content: str = converter.convert(protocol, speeches, strip_paths(input_filename))

    if output_filename is not None:
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, "w", encoding="utf-8") as fp:
            fp.write(content)
    else:
        echo(content, nl=False, err=False)
