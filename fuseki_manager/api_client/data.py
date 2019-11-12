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

    def upload_files(self, ds_name, file_paths):
        """Restore a list of data files to a dataset.

        :param str ds_name: Dataset's name.
        :param list[Path] file_paths: List of file's path to send.
        :returns dict: Details on data inserted, JSON format.
        :raises InvalidFileError:
        """
        uri = self._build_uri(ds_name, service_name='data')
        # build files parameter
        files = [
            ('file', build_http_file_obj(file_path, 'application/rdf+xml'))
            for file_path in file_paths]
        response = self._post(uri, files=files)
        return response.json()
