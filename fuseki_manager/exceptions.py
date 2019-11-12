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


class EmptyDBError(FusekiClientError):
    """No result error while quering data."""


class UniquenessDBError(FusekiClientError):
    """Not unique result error while quering data."""


class ArgumentError(FusekiClientError):
    """Bad argument error while quering data."""
