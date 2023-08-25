import gzip
import os

import pytest
from ccc import Corpus

from pyriksprot import interface, utility
from pyriksprot.corpus.tagged import load_protocol, load_protocols
from pyriksprot.workflows.export_vrt import VrtExportBatch, export_vrt

from .utility import RIKSPROT_REPOSITORY_TAG

jj = os.path.join


def test_ccc():
    # corpora: Corpora = Corpora(registry_path="/usr/local/share/cwb/registry/")
    corpus = Corpus(corpus_name="RIKSPROT_V060_TEST", registry_path="/usr/local/share/cwb/registry/")
    dump = corpus.query(
        '[lemma="Sverige"]',
        context_left=5,
        context_right=5,
    )
    df = dump.concordance()
    assert df is not None


def test_protocol_to_vrt():
    filename: str = f"tests/test_data/source/5files/{RIKSPROT_REPOSITORY_TAG}/tagged_frames/199192/prot-199192--127.zip"

    protocol: interface.Protocol = load_protocol(filename)

    assert protocol is not None

    vrt_str = protocol.to_vrt()

    assert vrt_str is not None

    tagged_str: str = protocol.tagged_text

    assert vrt_str == utility.xml_escape(tagged_str[tagged_str.find('\n') + 1 :])

    vrt_str = protocol.to_vrt('protocol', 'speech', 'utterance')
    assert '<protocol' in vrt_str
    assert '<speech' in vrt_str
    assert '<utterance' in vrt_str


def test_fake_protocols_to_vrt():
    filename: str = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}/tagged_frames/prot-1958-fake.zip'

    protocol: interface.Protocol = load_protocol(filename)
    tagged_str: str = protocol.tagged_text
    expected_protocol_vrt_str = utility.xml_escape(tagged_str[tagged_str.find('\n') + 1 :])

    assert protocol is not None

    vrt_str = protocol.to_vrt()

    assert vrt_str is not None

    assert vrt_str == expected_protocol_vrt_str

    vrt_str = protocol.to_vrt('protocol')

    assert (
        vrt_str
        == f'<protocol title="{protocol.name}" date="{protocol.date}">\n{expected_protocol_vrt_str}\n</protocol>'
    )

    vrt_str = protocol.to_vrt('protocol', 'speech')

    expected_vrt_str = """<protocol title="prot-1958-fake" date="1958">
<speech id="u-1" title="prot-1958-fake_001" who="olle" date="1958" page_number="0">
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
<speech id="u-3" title="prot-1958-fake_002" who="kalle" date="1958" page_number="0">
Jag	jag	PN	PN.UTR.SIN.DEF.SUB
heter	heta	VB	VB.PRS.AKT
Kalle	Kalle	PM	PM.NOM
.	.	MAD	MAD
</speech>
<speech id="u-4" title="prot-1958-fake_003" who="unknown" date="1958" page_number="0">
Olle	Olle	PM	PM.NOM
är	vara	VB	VB.PRS.AKT
snäll	snäll	JJ	JJ.POS.UTR.SIN.IND.NOM
.	.	MAD	MAD
Han	han	PN	PN.UTR.SIN.DEF.SUB
är	vara	VB	VB.PRS.AKT
snäll	snäll	JJ	JJ.POS.UTR.SIN.IND.NOM
!	!	MAD	MAD
</speech>
<speech id="u-6" title="prot-1958-fake_004" who="olle" date="1958" page_number="1">
Nej	nej	IN	IN
,	,	MID	MID
Kalle	Kalle	PM	PM.NOM
är	vara	VB	VB.PRS.AKT
dum	dum	JJ	JJ.POS.UTR.SIN.IND.NOM
.	.	MAD	MAD
</speech>
</protocol>"""
    assert vrt_str == expected_vrt_str

    vrt_str = protocol.to_vrt('protocol', 'speech', 'utterance')

    assert 'protocol' in vrt_str
    assert 'speech' in vrt_str
    assert 'utterance' in vrt_str

    with pytest.raises(ValueError):
        vrt_str = protocol.to_vrt('protocol', 'speech', 'apa')

    with pytest.raises(NotImplementedError):
        vrt_str = protocol.to_vrt('sentence')


def test_protocols_to_vrt():
    folder: str = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}/tagged_frames'

    protocols: interface.Protocol = load_protocols(folder)

    vrt_str = interface.Protocol.to_vrts(protocols)

    assert vrt_str
    assert 'protocol' not in vrt_str
    assert 'speech' not in vrt_str
    assert 'utterance' not in vrt_str
    assert all(p.to_vrt() in vrt_str for p in load_protocols(folder))
    assert '\n'.join(p.to_vrt() for p in load_protocols(folder)) + '\n' == vrt_str

    """Test returning VRT as string."""
    vrt_str = interface.Protocol.to_vrts(
        load_protocols(folder),
        "protocol",
        "speech",
        "utterance",
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
    interface.Protocol.to_vrts(
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
    interface.Protocol.to_vrts(
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


def test_to_cwb():
    pass


@pytest.mark.parametrize(
    'target,tags,processes',
    [
        ('-', ('protocol', 'speech', 'utterance'), 1),
        ('tests/output/fakes.vrt', ('protocol',), 2),
        ('tests/output/fakes.vrt.gz', ('protocol',), 1),
        ('tests/output/fakes.vrt.gz', [], 1),
    ],
)
def test_export_folder_batches(target: str | None, tags, processes):
    folder: str = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}/tagged_frames'
    batches: list[VrtExportBatch] = [VrtExportBatch(folder, target, "year", {'year': '2020', 'title': '202021'})]
    export_vrt(batches, *tags, processes=processes)

    pass
