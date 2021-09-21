import abc
import itertools
from dataclasses import dataclass
from functools import reduce
from typing import Any, Callable, List, Literal, Mapping, Union

from .utility import compress

TaggedDocument = Mapping[str, List[str]]
IterateLevel = Literal['protocol', 'speech', 'speaker', 'who', 'utterance', 'paragraph']


@dataclass
class ProtocolIterItem:

    name: str
    who: str
    id: str
    text: str
    page_number: str

    def __repr__(self) -> str:

        # text2 = zlib.decompress(base64.b64decode(text_base64)).decode('utf-8')

        return (
            f"{self.name or '*'}\t"
            f"{self.id or '?'}\t"
            f"{self.who or '*'}\t"
            f"{self.page_number or ''}\t"
            f"{len(self.text)/1024.0:.2f}kB\t"
        )

    def text_z64(self) -> bytes:
        """Compress text, return base64 encoded string."""
        return compress(self.text)


class IProtocol(abc.ABC):
    ...


class ITagger(abc.ABC):
    def __init__(self, preprocessors: Callable[[str], str] = None):
        self.preprocessors: Callable[[str], str] = preprocessors or []

    def tag(self, text: Union[str, List[str]], preprocess: bool = True) -> List[TaggedDocument]:
        """Tag text. Return dict if lists."""
        if isinstance(text, str):
            text = [text]

        if not isinstance(text, list):
            return ValueError("invalid type")

        if len(text) == 0:
            return []

        if preprocess:
            text: List[str] = [self.preprocess(d) for d in text]

        tagged_documents = self._tag(text)

        return tagged_documents

    @abc.abstractmethod
    def _tag(self, text: Union[str, List[str]]) -> List[TaggedDocument]:
        ...

    @abc.abstractmethod
    def _to_dict(self, tagged_document: Any) -> TaggedDocument:
        return {}

    @staticmethod
    def to_csv(tagged_document: TaggedDocument, sep='\t') -> str:
        """Converts a TaggedDocument to a TSV string"""

        tokens, lemmas, pos, xpos = (
            tagged_document['token'],
            tagged_document['lemma'],
            tagged_document['pos'],
            tagged_document['xpos'],
        )
        csv_str = '\n'.join(
            itertools.chain(
                [f"token{sep}lemma{sep}pos{sep}xpos"],
                (f"{tokens[i]}{sep}{lemmas[i]}{sep}{pos[i]}{sep}{xpos[i]}" for i in range(0, len(tokens))),
            )
        )
        return csv_str

    def preprocess(self, text: str) -> str:
        """Transform `text` with preprocessors."""
        text: str = reduce(lambda res, f: f(res), self.preprocessors, text)
        return text
