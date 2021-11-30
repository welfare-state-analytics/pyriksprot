#!/usr/bin/env python

# pylint: disable=no-else-continue, attribute-defined-outside-init consider-using-f-string

"""
 untangle

 Converts xml to python objects.

 The only method you need to call is parse()

 Partially inspired by xml2obj
 (http://code.activestate.com/recipes/149368-xml2obj/)

 Author: Christian Stefanescu (http://0chris.com)
 License: MIT License - http://www.opensource.org/licenses/mit-license.php
"""
import keyword
import os
from io import StringIO
from typing import Container, List, Mapping, Optional, Set
from xml.sax import handler, make_parser


def is_string(x):
    return isinstance(x, str)


__version__ = "1.1.1"


class Element:
    """
    Representation of an XML element.
    """

    def __init__(self, name: str, attributes: Mapping[str, str]):
        self.name: str = name
        self.attributes: Mapping[str, str] = attributes
        self.children: List["Element"] = []
        self.is_root: bool = False
        self.cdatas: List[str] = []

    @property
    def cdata(self):
        return ''.join(self.cdatas)

    @cdata.setter
    def cdata(self, cdata):
        self.cdatas = [cdata]

    def add_child(self, element: "Element") -> None:
        """ Store child elements. """
        self.children.append(element)

    def add_cdata(self, cdata: str) -> None:
        """ Store cdata """
        self.cdatas.append(cdata)

    def get_attribute(self, key) -> Optional[str]:
        """ Get attributes by key """
        return self.attributes.get(key)

    def get_elements(self, name: str = None) -> List["Element"]:
        """ Find a child element by name """
        if name:
            return [e for e in self.children if e.name == name]
        return self.children

    def __getitem__(self, key):
        return self.get_attribute(key)

    def __getattr__(self, key):
        matching_children = [x for x in self.children if x.name == key]
        if matching_children:
            if len(matching_children) == 1:
                self.__dict__[key] = matching_children[0]
                return matching_children[0]

            self.__dict__[key] = matching_children
            return matching_children

        raise AttributeError(f"'{self.name}' has no attribute '{key}'")

    def __hasattribute__(self, name):
        if name in self.__dict__:
            return True
        return any(x.name == name for x in self.children)

    def __iter__(self):
        yield self

    def __str__(self):
        return "Element <%s> with attributes %s, children %s and cdata %s" % (
            self.name,
            self.attributes,
            self.children,
            self.cdata,
        )

    def __repr__(self):
        return "Element(name = %s, attributes = %s, cdata = %s)" % (
            self.name,
            self.attributes,
            self.cdata,
        )

    def __nonzero__(self):
        return self.is_root or self.name is not None

    def __eq__(self, val: str):
        return self.cdata == val

    def __dir__(self):
        children_names = [x.name for x in self.children]
        return children_names

    def __len__(self):
        return len(self.children)

    def __contains__(self, key: str):
        return key in dir(self)


class Handler(handler.ContentHandler):
    """
    SAX handler which creates the Python object structure out of ``Element``s
    """

    def __init__(self, ignore_tags: Container[str]):
        super().__init__()

        self.root: Element = Element(None, None)
        self.root.is_root = True
        self.elements: List[Element] = []
        self.ignore_tags: Set[str] = set(ignore_tags or [])

    def startElement(self, name: str, attrs: Mapping[str, str]) -> None:

        if name in self.ignore_tags:
            return

        name = name.replace("-", "_").replace(".", "_").replace(":", "_")

        if keyword.iskeyword(name):
            name += "_"

        element = Element(name, dict(attrs))
        if len(self.elements) > 0:
            self.elements[-1].add_child(element)
        else:
            self.root.add_child(element)
        self.elements.append(element)

    def endElement(self, name: str) -> None:

        if name in self.ignore_tags:
            return

        self.elements.pop()

    def characters(self, content: str) -> None:
        # self.elements[-1].add_cdata(content)
        self.elements[-1].cdatas.append(content)


def parse(filename: str, ignore_tags: Container[str] = None, **parser_features) -> Element:
    """
    Interprets the given string as a filename, URL or XML data string,
    parses it and returns a Python object which represents the given
    document.

    Extra arguments to this function are treated as feature values to pass
    to ``parser.setFeature()``. For example, ``feature_external_ges=False``
    will set ``xml.sax.handler.feature_external_ges`` to False, disabling
    the parser's inclusion of external general (text) entities such as DTDs.

    Raises ``ValueError`` if the first argument is None / empty string.

    Raises ``AttributeError`` if a requested xml.sax feature is not found in
    ``xml.sax.handler``.

    Raises ``xml.sax.SAXParseException`` if something goes wrong
    during parsing.
    """
    if filename is None or (is_string(filename) and filename.strip()) == "":
        raise ValueError("parse() takes a filename, URL or XML string")
    parser = make_parser()
    for feature, value in parser_features.items():
        parser.setFeature(getattr(handler, feature), value)
    sax_handler: Handler = Handler(ignore_tags=ignore_tags)
    parser.setContentHandler(sax_handler)
    if is_string(filename) and (os.path.exists(filename) or is_url(filename)):
        parser.parse(filename)
    else:
        if hasattr(filename, "read"):
            parser.parse(filename)
        else:
            parser.parse(StringIO(filename))

    return sax_handler.root


def is_url(string: str) -> bool:
    """
    Checks if the given string starts with 'http(s)'.
    """
    try:
        return string.startswith("http://") or string.startswith("https://")
    except AttributeError:
        return False
