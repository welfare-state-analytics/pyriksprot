import contextlib
import functools
import gzip
import sys
import zipfile
from collections import namedtuple
from io import StringIO, TextIOWrapper
from typing import overload

import tqdm

from pyriksprot.corpus.tagged.persist import load_protocols
from pyriksprot.metadata.person import SpeakerInfo, SpeakerInfoService
from pyriksprot.utility import strip_csv_header, xml_escape

from .. import interface
from .. import to_speech as mu  # pylint: disable=import-outside-toplevel

VrtExportBatch = namedtuple('VrtExportBatch', 'source target tag attribs')

IEntity = interface.Protocol | interface.Speech | interface.Utterance


def vrt_exporter(func):
    @functools.wraps(func)
    def exporter(self, *tags, **kwargs):
        keys: set[str] = {e.value for e in interface.SegmentLevel}
        if any(tag not in keys for tag in tags):
            raise ValueError(f"Unknown tag: {tags}")

        return func(self, *tags, **kwargs)

    return exporter


class VrtExportService:
    def __init__(self, speaker_service: SpeakerInfoService, merge_strategy: str = "chain_consecutive_unknowns"):
        self.speaker_service: SpeakerInfoService = speaker_service
        self.merge_strategy: str = merge_strategy

    @overload
    def to_vrt(self, entity: interface.Utterance, *tags: tuple[str]) -> str:
        ...

    @overload
    def to_vrt(self, entity: interface.Speech, *tags: tuple[str]) -> str:
        ...

    @overload
    def to_vrt(self, entity: interface.Protocol, *tags: tuple[str]) -> str:
        ...

    def to_vrt(self, entity: IEntity, *tags: tuple[str]) -> str:
        if 'sentence' in tags:
            raise NotImplementedError("Export to VRT with sentence structural tags is not implemented yet.")
        if isinstance(entity, interface.Utterance):
            return self._utterance_to_vrt(entity, *tags)
        if isinstance(entity, interface.Speech):
            return self._speech_to_vrt(entity, *tags)
        if isinstance(entity, interface.Protocol):
            return self._protocol_to_vrt(entity, *tags)
        raise TypeError(f"Unknown entity type: {type(entity)}")

    # @vrt_exporter
    def _utterance_to_vrt(self, utterance: interface.Utterance, *tags: tuple[str]) -> str:
        if not tags:
            return xml_escape(strip_csv_header(utterance.annotation))

        # if 'sentence' in tags or 'paragraph' in tags:
        #     # TODO: Tagged data frames must contain sentence/paragraph markers
        #     # 1) Frame has a sentence/paragraph id column
        #     # 2) Or sentence or paragraph markers are included in the tagged text on separate lines
        #     raise NotImplementedError("Export to VRT with sentence/paragraph structural tags is not implemented yet.")

        vrt_str: str = xml_escape(strip_csv_header(utterance.annotation))

        if 'utterance' in tags:
            vrt_str = f'<utterance id="{utterance.u_id}" page_number="{utterance.page_number}" who="{utterance.who}">\n{vrt_str}</utterance>\n'

        return vrt_str

    # @vrt_exporter
    def _speech_to_vrt(self, speech: interface.Speech, *tags: tuple[str]) -> str:
        if tags is None:
            return xml_escape(strip_csv_header(speech.tagged_text))

        speaker_info: SpeakerInfo = self.speaker_service.get_speaker_info(
            u_id=speech.speech_id, person_id=speech.who, year=speech.get_year()
        )

        if speaker_info is None:
            speaker_info = SpeakerInfo.empty()

        vrt_str: str = '\n'.join(self.to_vrt(u, *tags) for u in speech.utterances)
        if 'speech' in tags:
            vrt_str: str = (
                '<speech '
                f'id="{speech.speech_id}" '
                f'title="{speech.document_name}" '
                f'who="{speech.who}" '
                f'date="{speech.speech_date}" '
                f'party_id="{speaker_info.party_id}" '
                f'gender_id="{speaker_info.gender_id}" '
                f'office_type_id="{speaker_info.term_of_office.office_type_id}" '
                f'sub_office_type_id="{speaker_info.term_of_office.sub_office_type_id}" '
                f'name="{speaker_info.name}" '
                f'page_number="{speech.page_number}"'
                '>\n'
                f'{vrt_str}\n'
                '</speech>'
            )

        return vrt_str

    # @vrt_exporter
    def _protocol_to_vrt(self, protocol: interface.Protocol, *tags: tuple[str]) -> str:
        """Export protocol to VRT format with optional structural tags."""

        """Local import do avoid circular dependency"""

        if tags is None:
            return xml_escape(strip_csv_header(protocol.tagged_text))

        vrt_str: str = ''
        if 'speech' in tags:
            speeches: list[interface.Speech] = mu.to_speeches(protocol=protocol, merge_strategy=self.merge_strategy)
            vrt_str: str = '\n'.join(self.to_vrt(s, *tags) for s in speeches)
        else:
            vrt_str: str = '\n'.join(self.to_vrt(u, *tags) for u in protocol.utterances)

        if 'protocol' in tags:
            vrt_str: str = f'<protocol title="{protocol.name}" date="{protocol.date}">\n{vrt_str}\n</protocol>'

        return vrt_str

    def to_vrts(
        self,
        protocols: list[interface.Protocol],
        *tags: tuple[str],
        output: None | str = None,
        outer_tag: str = None,
        **outer_tag_attribs,
    ) -> None | str:
        """Export multiple protocols to VRT format with optional structural tags.
        Return VRT string if output is None, otherwise write to file and return None."""

        if isinstance(output, str) and output.endswith("zip"):
            """Write each protocol to a separate file in a zip archive"""
            if outer_tag:
                raise ValueError("Cannot write outer tag when target is a single zip file with many protocols")

            with zipfile.ZipFile(output, 'w') as zink:
                for p in protocols:
                    zink.writestr(f"{p.name}.vrt", self.to_vrt(p, *tags))
                return None

        with _open_output(output) as zink:
            if outer_tag:
                zink.write(_xml_start_tag(outer_tag, **outer_tag_attribs) + '\n')
            for p in protocols:
                zink.write(f"{self.to_vrt(p, *tags)}\n")
            if outer_tag:
                zink.write(f'</{outer_tag}>')
            if isinstance(zink, StringIO):
                return zink.getvalue()
            return None


def _open_output(output: str) -> TextIOWrapper:
    if output == '-':
        return contextlib.nullcontext(sys.stdout)
    if output is None:
        return StringIO('', newline='')
    if output.endswith('.gz'):
        return gzip.open(output, 'wt', encoding="utf8")
    return open(output, 'w', encoding="utf8")


def _xml_start_tag(tag: str, **attribs) -> str:
    """Generate XML open tag with optional attributes."""
    attrib_str: str = " ".join((f"{k}=\"{v}\"" for k, v in attribs.items()))
    return f"<{tag} {attrib_str}>"


class VrtBatchExporter:
    """Export a batch(es) of protocols on disk to VRT format."""

    def __init__(
        self, speaker_service: str | SpeakerInfoService, merge_strategy: str = "chain_consecutive_unknowns", **opts
    ):
        self.merge_strategy: str = merge_strategy
        self.speaker_service: SpeakerInfoService = (
            speaker_service if isinstance(speaker_service, SpeakerInfoService) else SpeakerInfoService(speaker_service)
        )
        self.export_service: VrtExportService = VrtExportService(self.speaker_service, self.merge_strategy)
        self.opts: dict = opts

    @overload
    def export(self, data: tuple, *tags: tuple[str]) -> None:
        ...

    @overload
    def export(self, data: VrtExportBatch, *tags: tuple[str]) -> None:
        ...

    @overload
    def export(self, data: list[VrtExportBatch], *tags: tuple[str]) -> None:
        ...

    def export(self, data: tuple | VrtExportBatch | list[VrtExportBatch], *tags: tuple[str]) -> None:
        if isinstance(data, tuple):
            self.export(VrtExportBatch(*data), *tags)
        elif isinstance(data, VrtExportBatch):
            self._export_vrt_batch(data, *tags)
        elif isinstance(data, list):
            self._export_vrt_batches(data, *tags)
        else:
            raise TypeError(f"Unknown data type: {type(data)}")

    def _export_vrt_batch(self, batch: VrtExportBatch, tags: tuple[str]) -> None:
        """Export a batch of protocols to VRT format."""
        self.export_service.to_vrts(
            load_protocols(batch.source), *tags, output=batch.target, outer_tag=batch.tag, **batch.attribs
        )

    # def multi_export_batch(args) -> None:
    #     """Export a batch of protocols to VRT format."""
    #     export_batch(*args)

    def _export_vrt_batches(self, batches: list[VrtExportBatch], *tags: tuple[str]) -> None:
        """Export protocols to VRT format.
        Args:
            batches (list[tuple]): Protocols to export.
            tags (*str): Structural elements to write to VRT.
            # FIXME: Add support for root_tag and root_attribs
            tag (str): Root tag to use in VRT.
            date (str): Root tag attribute `date` to use in VRT.
            " DEPRECATED: processes (int): Number of processes to use.
        """
        assert self.opts.get('processes', 1) == 1, "Multiprocessing is not supported yet"
        # if processes > 1:
        #     args: list[tuple] = [(b, tags) for b in batches]
        #     with get_context("spawn").Pool(processes=processes) as executor:
        #         futures = executor.imap_unordered(multi_export_batch, args)
        #         deque(futures, maxlen=0)
        # else:
        for b in tqdm.tqdm(batches):
            self._export_vrt_batch(b, tags)
