# type: ignore

from . import metadata
from .corpus.corpus_index import CorpusSourceIndex, ICorpusSourceItem
from .corpus.iterate import ProtocolSegment, ProtocolSegmentIterator
from .dehyphenation import SwedishDehyphenator
from .dispatch import DispatchItem, SegmentMerger, create_grouping_hashcoder
from .interface import ParlaClarinError, Protocol, SegmentLevel, Speech, Utterance
from .preprocess import dedent, dehyphen, pretokenize
from .to_speech import MergerFactory, MergeStrategyType, to_speeches
from .utility import (
    compose,
    deprecated,
    dget,
    dotget,
    ensure_path,
    flatten,
    hasattr_path,
    is_empty,
    lookup,
    norm_join,
    parse_range_list,
    path_add_date,
    path_add_sequence,
    path_add_suffix,
    path_add_timestamp,
    reset_file,
    reset_folder,
    sanitize,
    slugify,
    strip_extensions,
    strip_path_and_extension,
    strip_paths,
    sync_delta_names,
    temporary_file,
    touch,
    ts_data_path,
    unlink,
    xml_escape,
)
from .workflows import compute_term_frequencies, extract_corpus_tags, extract_corpus_text
from .workflows.tag import (
    ITagger,
    ITaggerFactory,
    TaggedDocument,
    TaggerProvider,
    TaggerRegistry,
    tag_protocol,
    tag_protocol_xml,
    tag_protocols,
)
