"""Jena/Fuseki API client utils."""

import re
from io import BufferedIOBase, BytesIO
from pathlib import Path

from .exceptions import InvalidFileError


def build_http_file_obj(source, mime_type):
    """Build parameters representing a fiel stream for a POST request.

    :param file-like source: file-like object to send.
    :param str mime_type: MIME type of the file.
    :returns tuple: File object information.
    :raises InvalidFileError: When 'file_path' is not a file.

    Note : source could be :
    - a string representing path to file
    - a pathlib.Path representing path to file
    - a subclass of io.BufferedIOBase
    """
    if isinstance(source, (str, Path)):
        # ensure 'source' is instance of 'Path'
        source = Path(source)
        if not source.is_file():
            raise InvalidFileError(str(source))
        return (source.name, open(str(source), 'rb'), mime_type)

    if issubclass(source.__class__, BufferedIOBase):
        return ('unknown', source, mime_type)

    if isinstance(source, bytes):
        return ('unknown', BytesIO(source), mime_type)

    raise InvalidFileError(str(source))


def is_url(value):
    """Return whether or not given value is a valid URL."""

    regex = re.compile(
        r"^"
        # startchar <
        r"<?"
        # protocol identifier
        r"(?:(?:https?|ftp)://)"
        r"(?:"
        r"(localhost)"
        r"|"
        # host name
        r"(?:(?:[a-z\u00a1-\uffff0-9]-?)*[a-z\u00a1-\uffff0-9]+)"
        # domain name
        r"(?:\.(?:[a-z\u00a1-\uffff0-9]-?)*[a-z\u00a1-\uffff0-9]+)*"
        # TLD identifier
        r"(?:\.(?:[a-z\u00a1-\uffff]{2,}))"
        r")"
        # port number
        r"(?::\d{2,5})?"
        # resource path
        r"(?:/\S*)?"
        # query string
        r"(?:\?\S*)?"
        # endchar >
        r">?"
        r"$",
        re.UNICODE | re.IGNORECASE
    )
    pattern = re.compile(regex)
    return pattern.match(value)


def is_literal(value):
    chars = "\"\'"
    return \
        ":" not in value or \
        value[0] in chars and value[-1] in chars


def parse_url(value):
    if not value.startswith('<'):
        value = '<' + value
    if not value.endswith('>'):
        value = value + '>'
    return value


def parse_literal(value):
    chars = "\"\'"
    if value[0] not in chars:
        value = '"' + value
    if value[-1] not in chars:
        value = value + '"'
    return value
