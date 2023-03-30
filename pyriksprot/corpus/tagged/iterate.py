from __future__ import annotations

from typing import Iterable, List, Tuple

import pandas as pd

from .. import iterate
from . import persist


def multiprocessing_load(args) -> Iterable[iterate.ProtocolSegment]:
    protocol: iterate.Protocol = persist.load_protocol(filename=args[0])
    return (
        []
        if protocol is None
        else iterate.to_segments(
            protocol=protocol,
            content_type=args[1],
            segment_level=args[2],
            segment_skip_size=args[3],
            merge_strategy=args[4],
            which_year=args[5],
        )
    )


class ProtocolIterator(iterate.ProtocolSegmentIterator):
    """Reads xml files and returns a stream of `ProtocolSegment`"""

    def load(self, filename: str) -> List[Tuple[str, str, int]]:
        protocol: iterate.Protocol = persist.load_protocol(filename=filename)

        return (
            []
            if protocol is None
            else iterate.to_segments(
                protocol=protocol,
                content_type=self.content_type,
                segment_level=self.segment_level,
                segment_skip_size=self.segment_skip_size,
                merge_strategy=self.merge_strategy,
                which_year=self.which_year,
            )
        )

    def map_futures(self, imap, args):
        return imap(multiprocessing_load, args, chunksize=self.multiproc_chunksize)

    def to_segment_index(self) -> pd.DataFrame:
        """Iterate and return a meta data index over segments."""
        segment_index: pd.DataFrame = pd.DataFrame([x.to_dict() for x in self])
        return segment_index
