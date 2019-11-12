"""Jena/Fuseki API client utils."""

import re
from pathlib import Path

from .exceptions import InvalidFileError


def build_http_file_obj(file_path, mime_type):
    """Build parameters representing a fiel stream for a POST request.

    :param Path file_path: Path of the file to send.
    :param str mime_type: MIME type of the file.
    :returns tuple: File object information.
    :raises InvalidFileError: When 'file_path' is not a file.
    """
    # ensure 'file_path' is instance of 'Path'
    file_path = Path(file_path)
    if not file_path.is_file():
        raise InvalidFileError(str(file_path))
    return (file_path.name, open(str(file_path), 'rb'), mime_type)


def url_validator(value):
    """Return whether or not given value is a valid URL."""

    regex = re.compile(
        r"^"
        # protocol identifier
        r"(?:(?:https?|ftp)://)"
        r"(?:"
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
        r"$",
        re.UNICODE | re.IGNORECASE
    )
    pattern = re.compile(regex)
    return pattern.match(value)
