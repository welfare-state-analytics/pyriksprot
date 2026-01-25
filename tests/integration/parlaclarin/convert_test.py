from os.path import join as jj
from typing import List

import pytest

from pyriksprot import interface, to_speech
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus.parlaclarin import convert, parse


@pytest.mark.skip(reason="deprecated")
def test_convert_to_xml():
    fakes_folder: str = ConfigStore.config().get("fakes:folder")
    template_name: str = "speeches.xml.jinja"
    protocol: interface.Protocol = parse.ProtocolMapper.parse(jj(fakes_folder, "prot-1958-fake.xml"))

    speeches: List[interface.Speech] = to_speech.to_speeches(protocol=protocol, merge_strategy='chain')

    assert protocol is not None

    converter: convert.ProtocolConverter = convert.ProtocolConverter(template_name)

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="olle" speech_id="i-1" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
Jag heter Olle.
Vad heter du?
    </speech>
    <speech who="kalle" speech_id="i-3" speech_date="1958" speech_index="2">
Jag heter Kalle.
    </speech>
    <speech who="kalle" speech_id="i-4" speech_date="1958" speech_index="3">
Olle är snäll.
    </speech>
</protocol>"""

    assert result == expected

    protocol: interface.Protocol = parse.ProtocolMapper.parse(jj(fakes_folder, "prot-1958-fake.xml"))
    speeches: List[interface.Speech] = to_speech.to_speeches(protocol=protocol, merge_strategy='chain')

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="olle" speech_id="c01" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
    </speech>
    <speech who="olle" speech_id="c02" speech_date="1958" speech_index="2">
Jag heter Olle.
Vad heter du?
    </speech>
    <speech who="kalle" speech_id="c03" speech_date="1958" speech_index="3">
Jag heter Kalle.
Olle är snäll.
    </speech>
</protocol>"""
    assert result == expected

    protocol: interface.Protocol = parse.ProtocolMapper.parse(
        jj(fakes_folder, "prot-1958-fake.xml"),
    )
    speeches: List[interface.Speech] = to_speech.to_speeches(protocol=protocol, merge_strategy='who')

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="olle" speech_id="olle" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
Jag heter Olle.
Vad heter du?
    </speech>
    <speech who="kalle" speech_id="kalle" speech_date="1958" speech_index="2">
Jag heter Kalle.
Olle är snäll.
    </speech>
</protocol>"""
    assert result == expected
