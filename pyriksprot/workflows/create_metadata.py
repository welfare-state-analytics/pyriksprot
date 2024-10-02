import os

from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.metadata import database
from pyriksprot.metadata.schema import MetadataSchema

jj = os.path.join


def resolve_backend(db_opts: dict[str, dict[str, str]] | str | database.DatabaseInterface) -> md.DatabaseInterface:
    if isinstance(db_opts, database.DatabaseInterface):
        return db_opts
    if isinstance(db_opts, str):
        backend = database.SqliteDatabase
        opts: dict = {'filename': db_opts}
    elif isinstance(db_opts, dict):
        backend = db_opts['type']
        opts: dict = db_opts['options']
    else:
        raise ValueError(f"Invalid db_opts: {db_opts}")

    db: database.DatabaseInterface = database.create_backend(backend=backend, **opts)
    return db


def create_database_workflow(  # pylint: disable=too-many-arguments
    tag: str = None,
    metadata_folder: str = None,
    db_opts: dict[str, dict[str, str]] = None,
    gh_opts: dict[str, str] = None,
    corpus_folder: str = None,
    scripts_folder: str = None,
    skip_download_metadata: bool = False,
    skip_create_index: bool = True,
    skip_load_scripts: bool = False,
    force: bool = False,
) -> None:
    """Create a database from metadata configuration"""
    try:

        schema: MetadataSchema = MetadataSchema(tag)

        if skip_download_metadata or skip_create_index:
            schema: md.MetadataSchema = md.MetadataSchema(tag)
            if not schema.files_exist(metadata_folder):
                raise ValueError("metadata files must exist in source folder if download is skipped")

        if not skip_download_metadata:

            """Fetch metadata from github"""
            md.gh_fetch_metadata_folder(target_folder=metadata_folder, **gh_opts, tag=tag, force=force)

        if not skip_create_index:

            if not os.path.exists(corpus_folder or ""):
                raise ValueError("corpus_folder must be set to create indexes")

            """ Create index from corpus for protocols, utterances and speaker notes """
            index_service: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper, schema=schema)
            index_service.generate(corpus_folder=corpus_folder, target_folder=metadata_folder)

        db: database.DatabaseInterface = resolve_backend(db_opts)

        service: md.MetadataFactory = md.MetadataFactory(tag=tag, schema=schema, backend=db, **db.opts)

        """ Create database and upload metadata and index data."""
        service.create(force=force)

        """ Upload metadata to database """
        service.upload(schema, metadata_folder)

        service.verify_tag()

        if not skip_load_scripts:

            service.execute_sql_scripts(folder=scripts_folder)

    except Exception as ex:
        logger.error(ex)
        raise ex
