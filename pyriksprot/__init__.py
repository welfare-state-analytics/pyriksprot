# type: ignore

from .convert import ProtocolConverter, convert_protocol, dedent, dehyphen, pretokenize
from .dehyphenation import SwedishDehyphenator, SwedishDehyphenatorService
from .interface import ITagger, IterateLevel, TaggedDocument
from .iterators import IProtocolTextIterator, ProtocolTextIterator, XmlProtocolTextIterator
from .model import ParlaClarinError, Protocol, Speech, Utterance
from .parse import IterUtterance, ProtocolMapper, UtteranceMapper, XmlUntangleProtocol
from .persist import StorageFormat, load_metadata, load_protocol, store_protocol
from .tag import tag_protocol, tag_protocol_xml
from .tf import TermFrequencyCounter, compute_term_frequencies
from .utility import (
    data_path_ts,
    deprecated,
    dict_get_by_path,
    download_url,
    ensure_path,
    flatten,
    hasattr_path,
    load_dict,
    load_token_set,
    lookup,
    norm_join,
    path_add_date,
    path_add_sequence,
    path_add_suffix,
    path_add_timestamp,
    store_dict,
    store_token_set,
    strip_extensions,
    strip_path_and_add_counter,
    strip_path_and_extension,
    strip_paths,
    sync_delta_names,
    temporary_file,
    touch,
    ts_data_path,
    unlink,
)
