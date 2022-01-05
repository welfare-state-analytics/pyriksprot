import os
import sys
from typing import Any, Callable, Optional

import click

from pyriksprot import dispatch, interface

from .. import utility

CLI_LOG_PATH = './logs'


TARGET_TYPES = dispatch.IDispatcher.dispatcher_keys()
COMPRESS_TYPES = dispatch.CompressType.values()
CONTENT_TYPES = [e.value for e in interface.ContentType]
SEGMENT_LEVELS = ['protocol', 'speech', 'utterance', 'paragraph', 'who']


CLI_OPTIONS = {
    '--compress-type': dict(default='lzma', type=click.Choice(COMPRESS_TYPES), help='Compress type'),
    '--content-type': dict(default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Text or tags'),
    '--dedent': dict(default=False, is_flag=True, help='Remove indentation'),
    '--dehyphen': dict(default=False, is_flag=True, help='Dehyphen text'),
    '--group-key': dict(help='Partition key(s)', multiple=True, type=click.STRING),
    '--lowercase': dict(default=True, type=click.BOOL, is_flag=True, help='Lowercase tokem/text'),
    '--multiproc-keep-order': dict(default=False, is_flag=True, help='Process is sort order (slower, multiproc)'),
    '--multiproc-processes': dict(default=None, type=click.IntRange(1, 40), help='Number of processes to use'),
    '--segment-level': dict(default='who', type=click.Choice(SEGMENT_LEVELS), help='Protocol iterate level'),
    '--segment-skip-size': dict(default=1, type=click.IntRange(1, 1024), help='Skip smaller than threshold'),
    '--skip-lemma': dict(default=False, type=click.BOOL, is_flag=True, help='Skip lemma'),
    '--skip-puncts': dict(default=False, type=click.BOOL, is_flag=True, help='Skip puncts'),
    '--skip-stopwords': dict(default=False, type=click.BOOL, is_flag=True, help='Skip stopwords'),
    '--skip-text': dict(default=False, type=click.BOOL, is_flag=True, help='Skip text'),
    '--target-type': dict(
        default='single-id-tagged-frame-per-group', type=click.Choice(TARGET_TYPES), help='Target type'
    ),
    '--temporal-key': dict(default=None, help='Temporal partition key(s)', type=click.STRING),
    '--years': dict(default=None, help='Years to include in output', type=click.STRING),
    '--force': dict(default=False, help='Force remove of existing files', is_flag=True),
}


def update_arguments_from_options_file(
    *,
    arguments: dict,
    filename_key: str,
    log_args: bool = True,
    ctx: click.Context = None,
    skip_keys: str = 'ctx,options_filename',
) -> dict:
    """Updates `arguments` based on values found in file specified by `filename_key`.
    Values specified at the command line overrides values from options file."""

    options_filename: Optional[str] = arguments.get(filename_key)
    del arguments[filename_key]

    arguments = utility.update_dict_from_yaml(options_filename, arguments)
    arguments.update(passed_cli_arguments(ctx, arguments))

    for k in skip_keys.split(','):
        if k in arguments:
            del arguments[k]

    if log_args:
        log_arguments(arguments)

    return arguments


def log_arguments(args: dict, subdir: bool = False, skip_keys: str = 'ctx,options_filename') -> None:
    def fix_value(v: Any):
        if isinstance(v, tuple):
            v = list(v)
        # if isinstance(v, list):
        #     if len(v) == 1:
        #         v = v[0]
        return v

    cli_command: str = utility.strip_path_and_extension(sys.argv[0])

    log_dir: str = os.path.join(CLI_LOG_PATH, cli_command) if subdir else CLI_LOG_PATH

    os.makedirs(log_dir, exist_ok=True)

    log_name: str = utility.ts_data_path(log_dir, f"{cli_command}.yml")
    log_args: dict = {k: fix_value(v) for k, v in args.items() if k not in skip_keys.split(',')}
    utility.write_yaml(log_args, log_name)


def passed_cli_arguments(ctx: click.Context, args: dict) -> dict:
    """Return args specified in commande line"""
    ctx = ctx or click.get_current_context()
    cli_args = {
        name: args[name] for name in args if ctx.get_parameter_source(name) == click.core.ParameterSource.COMMANDLINE
    }

    return cli_args


def remove_none(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def option2(*param_decls: str, **attrs: Any) -> Callable[..., Any]:
    for opt_attrib in ('default', 'help', 'type', 'is_flag', 'multiple'):
        if opt_attrib not in attrs and any(p in CLI_OPTIONS for p in param_decls):
            opt_name: str = next(p for p in param_decls if p in CLI_OPTIONS)
            opt: dict = CLI_OPTIONS[opt_name]
            if opt_attrib in opt:
                attrs[opt_attrib] = opt[opt_attrib]
    return click.option(*param_decls, **attrs)
