"""Jena/Fuseki API client utils."""

from pathlib import Path

from .exceptions import InvalidFileError


def build_rdf_file_obj(file_path):
    """Build parameters representing a fiel stream for a POST request.

    :param Path file_path: Path of the file to send.
    :returns tuple: File object information.
    :raises InvalidFileError: If 'file_path' is not a file.
    """
    # ensure 'file_path' is instance of 'Path'
    file_path = Path(file_path)
    if not file_path.is_file():
        raise InvalidFileError(str(file_path))
    return (
        file_path.name, open(str(file_path), 'rb'), 'application/rdf+xml')
