# type: ignore

from .codecs import Codecs
from .config import (
    IDNAME2NAME_MAPPING,
    NAME2IDNAME_MAPPING,
    PARTY_COLOR_BY_ABBREV,
    PARTY_COLOR_BY_ID,
    PARTY_COLORS,
    RIKSPROT_METADATA_TABLES,
    MetadataTableConfig,
    MetadataTableConfigs,
)
from .generate import CorpusIndexFactory, DatabaseHelper
from .person import Person, PersonIndex, PersonParty, SpeakerInfo, SpeakerInfoService, TermOfOffice
from .repository import gh_dl_metadata, gh_dl_metadata_by_config, gh_dl_metadata_extra, gh_ls, gh_tags
from .subset import subset_to_folder
from .utility import fix_incomplete_datetime_series
from .utterance import UtteranceIndex
from .verify import ConfigConformsToFolderSpecification, ConfigConformsToTagSpecification, TagsConformSpecification
