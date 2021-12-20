import os
import pickle
from collections import defaultdict
from glob import glob
from typing import Any, Callable, Dict, Iterable, List, Union

from tqdm.auto import tqdm

from ..foss.sparv_tokenize import default_tokenize
from .iterate import XmlProtocolSegmentIterator, XmlUntangleSegmentIterator


class TermFrequencyCounter:
    """Term frequency container.

    Attributes:
        frequencies (Dict[str, int]): Term frequencies.
    """

    def __init__(self, tokenizer: Callable[[str], List[str]] = None, do_lower_case: bool = True, progress: bool = True):
        """
        Args:
            tokenizer (Callable[[str], List[str]], optional): Tokenizer to use when ingesting tokens. Defaults to None.
            do_lower_case (bool, optional): [description]. Defaults to True.
        """

        self._tokenize: Callable[[Any], List[str]] = tokenizer or default_tokenize
        self._do_lower_case: bool = do_lower_case
        self._progress: bool = progress

        self.frequencies: Dict[str, int] = defaultdict(int)

    def ingest(self, value: Union[str, Iterable[str], XmlUntangleSegmentIterator]) -> "TermFrequencyCounter":
        """Update term frequencies with term counts in `value`"""
        texts = (
            value
            if isinstance(value, str)
            else (x.data for x in value)
            if isinstance(value, (XmlUntangleSegmentIterator, XmlProtocolSegmentIterator))
            else value
        )
        for text in tqdm(texts, disable=not self._progress):
            if self._do_lower_case:
                text = text.lower()
            for word in self._tokenize(text):
                self.frequencies[word] += 1
        return self

    def store(self, filename: str, cut_off: int = None) -> None:
        """Store term frequencies to`filename`."""
        if cut_off:
            frequencies = {w: c for w, c in self.frequencies if c > cut_off}
        else:
            frequencies = self.frequencies
        with open(filename, 'wb') as fp:
            pickle.dump(frequencies, fp, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(filename: str) -> defaultdict(int):
        """Load term frequency counts from pickled file `filename`."""
        with open(filename, 'rb') as fp:
            return pickle.load(fp)


def compute_term_frequencies(
    *,
    source: Union[str, List[str]],
    filename: str,
    segment_skip_size: int = 1,
    multiproc_processes: int = 2,
    multiproc_keep_order: bool = False,
    progress: bool = True,
) -> TermFrequencyCounter:
    """Compute (corpus) term frequency for documents in `source `.

    Args:
        source (Union[str, List[str]]): ParlaClarin filename(s), folder, filename patterna
        filename (str): [description]

    Raises:
        ValueError: Unsupported source.

    Returns:
        TermFrequencyCounter: Combinded term frequencies for given source(s).
    """
    try:
        if isinstance(source, XmlUntangleSegmentIterator):
            texts = source
        else:
            if isinstance(source, str):
                if os.path.isfile(source):
                    filenames = [source]
                elif os.path.isdir(source):
                    filenames = glob(os.path.join(source, "*.xml"))
                else:
                    filenames = glob(source)
            elif isinstance(source, list):
                filenames = source
            else:
                raise ValueError(f"unknown source of type {type(source)}")

            texts = XmlUntangleSegmentIterator(
                filenames=filenames,
                segment_level=None,
                multiproc_processes=multiproc_processes,
                segment_skip_size=segment_skip_size,
                multiproc_keep_order=multiproc_keep_order,
            )

        counter = TermFrequencyCounter(progress=progress)

        counter.ingest(texts)

        if filename is not None:
            counter.store(filename)

        return counter

    except Exception as ex:
        print(ex)
        raise
