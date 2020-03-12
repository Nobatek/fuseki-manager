"""Jena/Fuseki data API client to manage data."""

from .base import FusekiBaseClient
from ..utils import build_http_file_obj


class FusekiDataClient(FusekiBaseClient):
    """Fuseki 'data' API client (data service)."""

    def _build_uri(self, ds_name, *, service_name=None):
        """Build service URI.

        :param str ds_name: Dataset's name used in URI.
        :returns str: Service's absolute URI.
        """
        uri = '{}{}'.format(self._base_uri, ds_name)
        if service_name is not None:
            uri = '{}/{}'.format(uri, service_name)
        return uri

    def drop_all(self, ds_name):
        """Remove all data on dataset by sending a 'DROP ALL' update query.

        :param str ds_name: Dataset's name.
        :returns bool: True if all data is removed without errors.
        """
        uri = self._build_uri(ds_name, service_name='update')
        query_params = {'update': 'DROP ALL'}
        self._post(uri, data=query_params, expected_status=(200, 204,))
        return True

    def upload_files(self, ds_name, sources, src_mime_type=None):
        """Restore a list of data files to a dataset.

        :param str ds_name: Dataset's name.
        :param list[Path] sources: List of file's path to send.
        :returns dict: Details on data inserted, JSON format.
        :raises InvalidFileError:
        """
        uri = self._build_uri(ds_name, service_name='data')
        src_mime_type = 'application/rdf+xml' \
            if src_mime_type is None else src_mime_type

        # build files parameter
        files = [
            ('file', build_http_file_obj(src, src_mime_type))
            for src in sources]
        response = self._post(uri, files=files)
        return response.json()
