import os
import shutil
from os.path import basename, dirname, exists, join, splitext

from loguru import logger

from pyriksprot import corpus as pc
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.corpus.utility import ls_corpus_folder
from pyriksprot.utility import replace_extension, reset_folder
from pyriksprot.workflows import tf

from .create_metadata import create_database_workflow

# pylint: disable=too-many-arguments


def subset_vrt_corpus(global_vrt_folder: str, local_xml_folder: str, local_vrt_folder: str) -> None:
    """Given a local XML folder, copy the global VRT (tagged frame) for all protocols that exists in the local parlaclarin folder"""

    for filename in ls_corpus_folder(local_xml_folder):
        subfolder: str = basename(dirname(filename))

        global_vrt_filename: str = join(global_vrt_folder, subfolder, f'{splitext(basename(filename))[0]}.zip')

        if not exists(global_vrt_filename):
            raise Exception(f'subset_vrt_corpus: file {global_vrt_filename} does not exist (cannot proceed)')

        target_vrt_filename: str = join(local_vrt_folder, subfolder, f'{splitext(basename(filename))[0]}.zip')

        os.makedirs(dirname(target_vrt_filename), exist_ok=True)

        shutil.copyfile(global_vrt_filename, target_vrt_filename)

        logger.info(f"copied {filename}")


def subset_corpus_and_metadata(
    *,
    corpus_version: str | None = None,
    metadata_version: str | None = None,
    corpus_folder: str | None = None,
    metadata_folder: str | None = None,
    documents: list[str] | str = None,
    global_corpus_folder: str | None = None,
    global_metadata_folder: str | None = None,
    target_root_folder: str | None = None,
    scripts_folder: str | None = None,
    gh_metadata_opts: dict[str, str] = None,
    gh_records_opts: dict[str, str] = None,
    db_opts: dict[str, str | dict[str, str]] = None,
    tf_filename: str | None = None,
    skip_download: bool = False,
    force: bool = True,
):
    """Subset metadata to folder `target_folder`/tag"""
    root_folder: str = join(target_root_folder or "", corpus_version or "")

    temp_folder: str = join(root_folder, "tmp")
    metadata_temp_folder: str = join(root_folder, "tmp/metadata")

    logger.info(f"Temp metadata folder {metadata_temp_folder}")
    logger.info(f"Global metadata folder {global_metadata_folder}")

    if isinstance(documents, str):
        documents = load_document_patterns(documents, extension='xml')

    schema: md.MetadataSchema = md.MetadataSchema(metadata_version)

    if force or not schema.files_exist(metadata_temp_folder):
        if not skip_download:
            logger.info(f"Downloading metadata from Github to {metadata_temp_folder}")
            schema: md.MetadataSchema = md.MetadataSchema(metadata_version)
            md.gh_download_folder(
                target_folder=metadata_temp_folder,
                **gh_metadata_opts,
                tag=metadata_version,
                force=True,
                extras=schema.extras_urls,
            )
        else:
            logger.info(f"Copying metadata from {global_metadata_folder} to {metadata_temp_folder}")
            reset_folder(temp_folder, force=True)
            shutil.rmtree(metadata_temp_folder, ignore_errors=True)
            shutil.copytree(global_metadata_folder, metadata_temp_folder)

            logger.info(f"Downloading extra URLs {schema.extras_urls} to {metadata_temp_folder}")
            md.gh_download_files(metadata_temp_folder, tag=metadata_version, errors='raise', items=schema.extras_urls)

    if not files_exist(documents, corpus_folder):
        if not skip_download:
            pc.download_protocols(
                filenames=documents,
                target_folder=corpus_folder,
                create_subfolder=True,
                tag=corpus_version,
                **gh_records_opts,
            )
        else:
            pc.copy_protocols(
                source_folder=global_corpus_folder,
                filenames=documents,
                target_folder=corpus_folder,
            )
        pc.create_tei_corpus_xml(source_folder=corpus_folder)

        # for filename in glob(join(root_folder, "resources", "*.xml")):
        #     shutil.copy(filename, protocols_target_folder)

    if force or not exists(tf_filename):
        tf.compute_term_frequencies(source=corpus_folder, filename=tf_filename)

    md.subset_to_folder(
        parser=ProtocolMapper,
        corpus_version=corpus_version,
        metadata_version=metadata_version,
        protocols_source_folder=corpus_folder,
        source_folder=metadata_temp_folder,
        target_folder=metadata_folder,
    )

    create_database_workflow(
        corpus_version=corpus_version,
        metadata_version=metadata_version,
        metadata_folder=metadata_folder,
        corpus_folder=corpus_folder,
        db_opts=db_opts,
        gh_opts=None,
        scripts_folder=scripts_folder,
        skip_create_index=False,
        skip_download_metadata=True,
        skip_load_scripts=False,
        force=force,
    )

    shutil.rmtree(path=metadata_temp_folder, ignore_errors=True)


def files_exist(filenames: list[str], folder: str) -> bool:
    return all(exists(join(folder, f)) for f in filenames)


def load_document_patterns(filename: str, extension: str = None) -> list[str]:
    """Loads a list of ParlaCLARIN document names/patterns from a file"""
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)

    patterns: list[str] = []
    with open(filename, "r", encoding="utf8") as fp:
        for pattern in fp.read().splitlines():
            if not pattern:
                continue
            if extension and '*' not in pattern:
                pattern: str = replace_extension(pattern, extension)
            patterns.append(pattern)

    return patterns
