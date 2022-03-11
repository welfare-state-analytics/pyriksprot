# type: ignore

from . import metadata
from .cluster import MergerFactory, MergeStrategyType, to_speeches
from .corpus.parlaclarin import pretokenize
from .corpus_index import CorpusSourceIndex, CorpusSourceItem
from .dehyphenation import SwedishDehyphenator, SwedishDehyphenatorService
from .interface import ParlaClarinError, Protocol, SegmentLevel, Speech, Utterance
from .merge import ProtocolSegmentGroup, SegmentCategoryClosed, SegmentMerger, create_grouping_hashcoder
from .segment import ProtocolSegment, ProtocolSegmentIterator
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
from .workflows import compute_term_frequencies, extract_corpus_tags, extract_corpus_text
from .workflows.tag import ITagger, TaggedDocument, tag_protocol, tag_protocol_xml
