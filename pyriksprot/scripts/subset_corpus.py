from __future__ import annotations

import os
import sys
import warnings
from typing import Any

import click
from click.testing import CliRunner

from pyriksprot.configuration.inject import ConfigStore, ConfigValue
from pyriksprot.workflows import subset_corpus_and_metadata

jj = os.path.join
relpath = os.path.relpath

warnings.filterwarnings("ignore", category=FutureWarning)


@click.command()
@click.argument('config_filename', type=str)
@click.argument('documents', type=str)
@click.argument('target-folder', type=str)
@click.option(
    '--skip-download', is_flag=True, type=bool, default=False, help="Skip Github download, use global data instead."
)
def main(
    config_filename: str = None,
    documents: list[str] | str = None,
    target_folder: str = None,
    skip_download: bool = True,
):
    print(locals())
    ConfigStore.configure_context(source=config_filename)

    corpus_version: str = ConfigValue("corpus:version", mandatory=True).resolve()
    metadata_version: str = ConfigValue("metadata:version", mandatory=True).resolve()
    corpus_folder: str = ConfigValue("corpus:folder", mandatory=True).resolve()
    metadata_folder: str = ConfigValue("metadata:folder", mandatory=True).resolve()

    global_corpus_folder: str = ConfigValue("global.corpus.folder", mandatory=skip_download).resolve()
    global_metadata_folder: str = ConfigValue("global.metadata.folder", mandatory=skip_download).resolve()

    gh_metadata_opts: str = ConfigValue("metadata.github", mandatory=not skip_download).resolve()
    gh_records_opts: str = ConfigValue("corpus.github", mandatory=not skip_download).resolve()
    db_opts: dict[str, Any] = ConfigValue("metadata.database", mandatory=True).resolve()
    tf_filename: str = ConfigValue("dehyphen.tf_filename", mandatory=True).resolve()

    if target_folder.startswith("/"):
        if target_folder.split("/")[1] == global_corpus_folder.split("/")[1]:
            raise ValueError(
                f"safety check: {target_folder} cannot be a subfolder of global corpus folder {global_corpus_folder}"
            )

    try:
        ConfigStore().configure_context(source=config_filename)
        subset_corpus_and_metadata(
            corpus_version=corpus_version,
            metadata_version=metadata_version,
            corpus_folder=corpus_folder,
            metadata_folder=metadata_folder,
            documents=documents,
            global_corpus_folder=global_corpus_folder,
            global_metadata_folder=global_metadata_folder,
            target_root_folder=target_folder,
            scripts_folder=None,
            gh_metadata_opts=gh_metadata_opts,
            gh_records_opts=gh_records_opts,
            db_opts=db_opts,
            tf_filename=tf_filename,
            skip_download=skip_download,
            force=True,
        )
    except Exception as ex:
        click.echo(ex)
        raise
        # sys.exit(1)


if __name__ == "__main__":

    if sys.gettrace() is not None:
        print("NOTE! click.testing.CliRunner")
        folder: str = "/home/roger/source/welfare-state-analytics/pyriksprot/tests/test_data/source/5files"
        runner = CliRunner()
        result = runner.invoke( main, [ f'{folder}/config_v1.4.1.yml', f'{folder}/protocols.txt', f'{folder}', '--skip-download' ] )
    else:
        main()
