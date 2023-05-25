from collections import deque, namedtuple
from multiprocessing import get_context

import tqdm

from pyriksprot.corpus.tagged.persist import load_protocols

from .. import interface

VrtExportBatch = namedtuple('VrtExportBatch', 'title source target')


def export_batch(batch: VrtExportBatch, tags, tag: str, date: str):
    """Export a batch of protocols to VRT format."""
    protocols: interface.Protocol = load_protocols(batch.source)
    interface.Protocol.to_vrts(
        protocols,
        *tags,
        output=batch.target,
        tag=tag,
        title=batch.title,
        date=date,
    )


def multi_export_batch(args):
    """Export a batch of protocols to VRT format."""
    return export_batch(*args)


def export_vrt(
    batches: list[VrtExportBatch],
    *tags: tuple[str],
    tag: str = None,
    date: str = None,
    processes: int = 1,
) -> None:
    """Export protocols to VRT format.
    Args:
        batches (list[tuple]): Protocols to export.
        tags (*str): Structural elements to write to VRT.
        tag (str): Root tag to use in VRT.
        date (str): Root tag attribute `date` to use in VRT.
        processes (int): Number of processes to use.
    """
    if processes > 1:
        args: list[tuple] = [(b, tags, tag, date) for b in batches]
        with get_context("spawn").Pool(processes=processes) as executor:
            futures = executor.imap_unordered(multi_export_batch, args)
            deque(futures, maxlen=0)
    else:
        for b in tqdm.tqdm(batches):
            export_batch(b, tags, tag, date)
