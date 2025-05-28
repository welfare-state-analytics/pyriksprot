# type: ignore

from .codecs import Codecs
from .corpus_index_factory import CorpusIndexFactory
from .database import DatabaseInterface, DefaultDatabaseType, SqliteDatabase, create_backend
from .download import gh_download_by_config, gh_download_files, gh_download_folder
from .metadata_factory import MetadataFactory
from .person import Person, PersonIndex, PersonParty, SpeakerInfo, SpeakerInfoService, TermOfOffice
from .schema import (
    IDNAME2NAME_MAPPING,
    NAME2IDNAME_MAPPING,
    PARTY_COLOR_BY_ABBREV,
    PARTY_COLOR_BY_ID,
    PARTY_COLORS,
    MetadataSchema,
    MetadataTable,
)
from .subset import subset_to_folder
from .utility import fix_incomplete_datetime_series
from .utterance import UtteranceIndex
from .verify import ConfigConformsToFolderSpecification, ConfigConformsToTagSpecification, TagsConformSpecification
