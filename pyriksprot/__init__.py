# type: ignore

from . import metadata, parlaclarin
from .corpus_index import CorpusSourceIndex, CorpusSourceItem
from .dehyphenation import SwedishDehyphenator, SwedishDehyphenatorService
from .interface import ParlaClarinError, Protocol, SegmentLevel, Speech, Utterance
from .merge_segments import ProtocolSegmentGroup, SegmentCategoryClosed, SegmentMerger, create_grouping_hashcoder
from .merge_utterances import MergeSpeechStrategyType, SpeechMergerFactory, to_speeches
from .parlaclarin import compute_term_frequencies, pretokenize
from .segment import ProtocolSegment, ProtocolSegmentIterator
from .tag import ITagger, TaggedDocument, tag_protocol, tag_protocol_xml
from .utility import (
    compose,
    data_path_ts,
    dedent,
    deprecated,
    dict_get_by_path,
    download_protocols,
    download_url,
    ensure_path,
    flatten,
    hasattr_path,
    is_empty,
    load_dict,
    load_token_set,
    lookup,
    norm_join,
    parse_range_list,
    path_add_date,
    path_add_sequence,
    path_add_suffix,
    path_add_timestamp,
    sanitize,
    slugify,
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
