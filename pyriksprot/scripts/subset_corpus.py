from __future__ import annotations

import os
import warnings
from typing import Any

import click

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

    tag: str = ConfigValue("version", mandatory=True).resolve()

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
            tag=tag,
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
    # options = {
    #     'tag': 'v1.1.0',
    #     'target_root_folder': '/home/roger/source/swedeb/sample-data/data/random_sample_10files',
    #     'documents': '/home/roger/source/swedeb/sample-data/data/random_sample_10files/protocols.txt',
    #     'global_corpus_folder': '/data/riksdagen_corpus_data/riksdagen-records/data',
    #     'global_metadata_folder': '/data/riksdagen_corpus_data/riksdagen-persons/data',
    #     'scripts_folder': None,
    #     'gh_metadata_opts': {'user': 'swerik-project', 'repository': 'riksdagen-persons', 'path': 'data'},
    #     'gh_records_opts': {
    #         'user': 'swerik-project',
    #         'repository': 'riksdagen-records',
    #         'path': 'data',
    #         'local_folder': '/data/riksdagen_corpus_data/riksdagen-records',
    #     },
    #     'db_opts': {
    #         'type': 'pyriksprot.metadata.database.SqliteDatabase',
    #         'options': {
    #             'filename': '/home/roger/source/swedeb/sample-data/data/random_sample_10files/v1.1.0/riksprot_metadata.db'
    #         },
    #     },
    #     'tf_filename': '/home/roger/source/swedeb/sample-data/data/random_sample_10files/v1.1.0/dehyphen/word-frequencies.pkl',
    #     'skip_download': True,
    #     'force': True,
    # }
    # subset_corpus_and_metadata(**options)
    main()
