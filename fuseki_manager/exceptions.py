"""Jena/Fuseki API client exceptions."""


class FusekiClientError(Exception):
    """Fuseki API client default error."""


class FusekiClientResponseError(FusekiClientError):
    """A response error from Fuseki server."""


class DatasetAlreadyExistsError(FusekiClientError):
    """Dataset already exists error."""


class DatasetNotFoundError(FusekiClientError):
    """Dataset not found error."""


class TaskNotFoundError(FusekiClientError):
    """Task not found error."""


class InvalidFileError(FusekiClientError):
    """Not a file error while uploading data."""
