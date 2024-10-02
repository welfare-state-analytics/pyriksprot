import gzip
import os
from unittest.mock import MagicMock

import pytest

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import utility
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus.tagged import load_protocol, load_protocols
from pyriksprot.metadata import SpeakerInfo, SpeakerInfoService
from pyriksprot.workflows.export_vrt import VrtBatchExporter, VrtExportBatch, VrtExportService

jj = os.path.join

# pylint: disable=redefined-outer-name


@pytest.fixture
def speaker_service() -> SpeakerInfoService:
    speaker_service: SpeakerInfoService = MagicMock(
        spec=SpeakerInfoService,
        **{
            'get_speaker_info.return_value': SpeakerInfo(
                speech_id='dummy-0',
                person_id='u-123456',
                wiki_id='Q123456',
                name='Dummy',
                gender_id=1,
                party_id=8,
                term_of_office=md.TermOfOffice(start_date=1937, end_date=1959, office_type_id=1, sub_office_type_id=2),
            )
        },
    )
    return speaker_service


@pytest.fixture
def export_service(speaker_service) -> VrtExportService:
    service: VrtExportService = VrtExportService(speaker_service)
    return service


def test_protocol_to_vrt(export_service: VrtExportService):
    version: str = ConfigStore.config().get("corpus.version")
    filename: str = f'tests/test_data/fakes/{version}/tagged_frames/prot-1958-fake.zip'

    protocol: interface.Protocol = load_protocol(filename)

    assert protocol is not None

    vrt_str: str = export_service.to_vrt(protocol)

    assert vrt_str is not None

    tagged_str: str = protocol.tagged_text

    assert vrt_str == utility.xml_escape(tagged_str[tagged_str.find('\n') + 1 :])

    vrt_str: str = export_service.to_vrt(protocol, 'protocol', 'speech', 'utterance')  # type: ignore
    assert '<protocol' in vrt_str
    assert '<speech' in vrt_str
    assert '<utterance' in vrt_str


def test_fake_protocols_to_vrt(export_service: VrtExportService):
    version: str = ConfigStore.config().get("corpus.version")
    filename: str = f'tests/test_data/fakes/{version}/tagged_frames/prot-1958-fake.zip'

    protocol: interface.Protocol = load_protocol(filename)
    tagged_str: str = protocol.tagged_text
    expected_protocol_vrt_str = utility.xml_escape(tagged_str[tagged_str.find('\n') + 1 :])

    assert protocol is not None

    vrt_str: str = export_service.to_vrt(protocol)

    assert vrt_str is not None

    assert vrt_str == expected_protocol_vrt_str

    vrt_str = export_service.to_vrt(protocol, 'protocol')  # type: ignore

    assert (
        vrt_str
        == f'<protocol title="{protocol.name}" date="{protocol.date}">\n{expected_protocol_vrt_str}\n</protocol>'
    )

    mocked_attributes: str = 'party_id="8" gender_id="1" office_type_id="1" sub_office_type_id="2" name="Dummy"'

    vrt_str = export_service.to_vrt(protocol, 'protocol', 'speech')  # type: ignore
    expected_vrt_str = f"""<protocol title="prot-1958-fake" date="1958">
<speech id="u-1" title="prot-1958-fake_001" who="olle" date="1958" {mocked_attributes} page_number="0">
Hej	hej	IN	IN
!	!	MID	MID
Detta	detta	PN	PN.NEU.SIN.DEF.SUB+OBJ
är	vara	VB	VB.PRS.AKT
en	en	DT	DT.UTR.SIN.IND
mening	mening	NN	NN.UTR.SIN.IND.NOM
.	.	MAD	MAD
Jag	jag	PN	PN.UTR.SIN.DEF.SUB
heter	heta	VB	VB.PRS.AKT
Olle	Olle	PM	PM.NOM
.	.	MID	MID
Vad	vad	HP	HP.NEU.SIN.IND
heter	heta	VB	VB.PRS.AKT
du	du	PN	PN.UTR.SIN.DEF.SUB
?	?	MAD	MAD
</speech>
<speech id="u-3" title="prot-1958-fake_002" who="kalle" date="1958" {mocked_attributes} page_number="0">
Jag	jag	PN	PN.UTR.SIN.DEF.SUB
heter	heta	VB	VB.PRS.AKT
Kalle	Kalle	PM	PM.NOM
.	.	MAD	MAD
</speech>
<speech id="u-4" title="prot-1958-fake_003" who="unknown" date="1958" {mocked_attributes} page_number="0">
Olle	Olle	PM	PM.NOM
är	vara	VB	VB.PRS.AKT
snäll	snäll	JJ	JJ.POS.UTR.SIN.IND.NOM
.	.	MAD	MAD
Han	han	PN	PN.UTR.SIN.DEF.SUB
är	vara	VB	VB.PRS.AKT
snäll	snäll	JJ	JJ.POS.UTR.SIN.IND.NOM
!	!	MAD	MAD
</speech>
<speech id="u-6" title="prot-1958-fake_004" who="olle" date="1958" {mocked_attributes} page_number="1">
Nej	nej	IN	IN
,	,	MID	MID
Kalle	Kalle	PM	PM.NOM
är	vara	VB	VB.PRS.AKT
dum	dum	JJ	JJ.POS.UTR.SIN.IND.NOM
.	.	MAD	MAD
</speech>
</protocol>"""
    assert vrt_str == expected_vrt_str

    vrt_str = export_service.to_vrt(protocol, 'protocol', 'speech', 'utterance')  # type: ignore

    assert 'protocol' in vrt_str
    assert 'speech' in vrt_str
    assert 'utterance' in vrt_str

    # with pytest.raises(ValueError):
    #     vrt_str = export_service.to_vrt(protocol, 'protocol', 'speech', 'apa')

    with pytest.raises(NotImplementedError):
        vrt_str = export_service.to_vrt(protocol, 'sentence')  # type: ignore


def test_protocols_to_vrts(export_service: VrtExportService):
    version: str = ConfigStore.config().get("corpus.version")
    folder: str = f'tests/test_data/fakes/{version}/tagged_frames'

    protocols: interface.Protocol = load_protocols(folder)

    vrt_str = export_service.to_vrts(protocols)

    assert vrt_str
    assert 'protocol' not in vrt_str
    assert 'speech' not in vrt_str
    assert 'utterance' not in vrt_str
    assert all(export_service.to_vrt(p) in vrt_str for p in load_protocols(folder))
    assert '\n'.join(export_service.to_vrt(p) for p in load_protocols(folder)) + '\n' == vrt_str

    """Test returning VRT as string."""
    vrt_str = export_service.to_vrts(
        load_protocols(folder),
        "protocol",
        "speech",
        "utterance",
        output=None,
        outer_tag="corpus",
        title="test-corpus",
        date="2020-01-01",
    )
    assert vrt_str

    assert 'protocol' in vrt_str
    assert 'speech' in vrt_str
    assert 'utterance' in vrt_str
    assert 'corpus' in vrt_str
    assert 'test-corpus' in vrt_str

    """Test writing to file (should yield same result as above))"""
    export_service.to_vrts(
        load_protocols(folder),
        "protocol",
        "speech",
        "utterance",
        output="tests/output/test_corpus.vrt",
        outer_tag="corpus",
        title="test-corpus",
        date="2020-01-01",
    )
    with open("tests/output/test_corpus.vrt", "r", encoding="utf8") as f:
        vrt_str_file: str = f.read()
        assert vrt_str_file == vrt_str

    """Test writing to file (should yield same result as above))"""
    export_service.to_vrts(
        load_protocols(folder),
        "protocol",
        "speech",
        "utterance",
        output="tests/output/test_corpus.vrt.gz",
        outer_tag="corpus",
        title="test-corpus",
        date="2020-01-01",
    )
    assert os.path.exists("tests/output/test_corpus.vrt.gz")
    with gzip.open("tests/output/test_corpus.vrt.gz", "rt", encoding="utf8", newline='') as f:
        vrt_str_file: str = f.read()
        assert vrt_str_file == vrt_str


@pytest.mark.parametrize(
    'target,tags',
    [
        ('-', ('protocol', 'speech', 'utterance')),
        ('tests/output/fakes.vrt', ('protocol',)),
        ('tests/output/fakes.vrt.gz', ('protocol')),
        ('tests/output/fakes.vrt.gz', []),
    ],
)
def test_export_vrt(target: str | None, tags, speaker_service: VrtExportService):
    version: str = ConfigStore.config().get("corpus.version")
    exporter = VrtBatchExporter(speaker_service=speaker_service)
    folder: str = f'tests/test_data/fakes/{version}/tagged_frames'
    batches: list[VrtExportBatch] = [VrtExportBatch(folder, target, "year", {'year': '2020', 'title': '202021'})]
    exporter.export(batches, *tags)

    assert target == "-" or os.path.exists(target)
