from typing import List

import pytest

from pyriksprot import interface
from pyriksprot.parlaclarin import convert, parse


@pytest.mark.skip(reason="deprecated")
def test_convert_to_xml():

    template_name: str = "speeches.xml.jinja"
    protocol: interface.Protocol = parse.ProtocolMapper.to_protocol("tests/test_data/fake/prot-1958-fake.xml")
    speeches: List[interface.Speech] = protocol.to_speeches(merge_strategy='who_sequence')

    assert protocol is not None

    converter: convert.ProtocolConverter = convert.ProtocolConverter(template_name)

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="A" speech_id="i-1" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
Jag heter Ove.
Vad heter du?
    </speech>
    <speech who="B" speech_id="i-3" speech_date="1958" speech_index="2">
Jag heter Adam.
    </speech>
    <speech who="B" speech_id="i-4" speech_date="1958" speech_index="3">
Ove är dum.
    </speech>
</protocol>"""

    assert result == expected

    protocol: interface.Protocol = parse.ProtocolMapper.to_protocol("tests/test_data/fake/prot-1958-fake.xml")
    speeches: List[interface.Speech] = protocol.to_speeches(merge_strategy='who_sequence')

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="A" speech_id="c01" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
    </speech>
    <speech who="A" speech_id="c02" speech_date="1958" speech_index="2">
Jag heter Ove.
Vad heter du?
    </speech>
    <speech who="B" speech_id="c03" speech_date="1958" speech_index="3">
Jag heter Adam.
Ove är dum.
    </speech>
</protocol>"""
    assert result == expected

    protocol: interface.Protocol = parse.ProtocolMapper.to_protocol(
        "tests/test_data/fake/prot-1958-fake.xml",
    )
    speeches: List[interface.Speech] = protocol.to_speeches(merge_strategy='who')

    result: str = converter.convert(protocol, speeches, "prot-200203--18.xml")

    expected = """<?xml version="1.0" encoding="UTF-8"?>
<protocol name="prot-1958-fake" date="1958">
    <speech who="A" speech_id="A" speech_date="1958" speech_index="1">
Hej! Detta är en mening.
Jag heter Ove.
Vad heter du?
    </speech>
    <speech who="B" speech_id="B" speech_date="1958" speech_index="2">
Jag heter Adam.
Ove är dum.
    </speech>
</protocol>"""
    assert result == expected
