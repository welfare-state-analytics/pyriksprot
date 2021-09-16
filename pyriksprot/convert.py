"""Convert ParlaClarin XML protocol to other text format using Jinja."""
import os
import textwrap
from typing import List, Union

from click import echo
from jinja2 import Environment, PackageLoader, Template, Undefined, select_autoescape

from . import model, parse
from .dehyphenation import SwedishDehyphenatorService
from .foss.sparv_tokenize import default_tokenize
from .utility import strip_paths

__dehyphenator: SwedishDehyphenatorService = None


def get_dehyphenator() -> SwedishDehyphenatorService:
    return __dehyphenator


def set_dehyphenator(**opts) -> None:
    global __dehyphenator
    __dehyphenator = SwedishDehyphenatorService(**opts)


def dedent(text: str) -> str:
    """Remove any white-space indentation from `text`."""
    if isinstance(text, Undefined):
        raise TypeError("dedent: jinja2.Undefined value string encountered")
    return textwrap.dedent(text) if text is not None else ""


def dehyphen(text: str) -> str:
    """Remove hyphens from `text`."""
    dehyphenated_text = get_dehyphenator().dehyphenator.dehyphen_text(text)
    return dehyphenated_text


def pretokenize(text: str) -> str:
    """Tokenize `text`, then join resulting tokens."""
    return ' '.join(default_tokenize(text))


jinja_env = Environment(
    loader=PackageLoader('pyriksprot.resources', 'templates'),
    autoescape=select_autoescape(['html', 'xml']),
    trim_blocks=True,
    lstrip_blocks=True,
)
jinja_env.filters['dedent'] = dedent
jinja_env.filters['dehyphen'] = dehyphen


class ProtocolConverter:
    """Transform ParlaClarin XML to template-based format."""

    def __init__(self, template: Union[str, Template]):
        """[summary]

        Args:
            template (Union[str, Template]): Jinja template.
        """
        global jinja_env

        if not template.endswith(".jinja"):
            template += ".jinja"

        if isinstance(template, str):
            template = jinja_env.get_template(template)

        self.template: Template = template

    def convert(self, protocol: model.Protocol, speeches: List[model.Speech], filename: str) -> str:
        """Transform `protocol` and return resulting text."""
        text: str = self.template.render(protocol=protocol, speeches=speeches, filename=filename)
        return text


def convert_protocol(
    input_filename: str = None,
    output_filename: str = None,
    template_name: str = None,
    merge_strategy: str = 'n',
    **dehyphen_cfg,
):
    """Convert protocol in `input_filename' using template `template_name`. Store result in `output_filename`.

    Args:
        input_filename (str, optional): Source file. Defaults to None.
        output_filename (str, optional): Target file. Defaults to None.
        template_name (str, optional): Template name (found in resource-folder). Defaults to None.
    """
    set_dehyphenator(**dehyphen_cfg)
    protocol: model.Protocol = parse.ProtocolMapper.to_protocol(input_filename, skip_size=5)
    content: str = ""

    if protocol.has_text():
        converter: ProtocolConverter = ProtocolConverter(template_name)
        speeches: List[model.Speech] = protocol.to_speeches(merge_strategy=merge_strategy)
        content: str = converter.convert(protocol, speeches, strip_paths(input_filename))

    if output_filename is not None:
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, "w", encoding="utf-8") as fp:
            fp.write(content)
    else:
        echo(content, nl=False, err=False)
