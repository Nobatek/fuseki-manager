"""Jena/Fuseki base API client to handle HTTP requests."""

import requests
from ..exceptions import (
    FusekiClientError, FusekiClientResponseError,
    DatasetNotFoundError)


class FusekiBaseClient():
    """Fuseki base API client, for both 'administration' and 'data' APIs."""

    def __init__(self, *, host='localhost', port=3030, is_secured=False,
                 user=None, pwd=None):
        """
        :param str host: Fuseki server host name. (default 'localhost')
        :param int port: Port used by Fuseki instance. (default 3030)
        :param bool is_secured:
            Should secured channel be used (https)? (default False)
        :param str user: User name used in BASIC authentication.
        :param str pwd: Password for BASIC authentication.
        """
        self.host = host
        self.port = port
        self.is_secured = is_secured
        self.auth_user = user
        self.auth_pwd = pwd

        self._auth_data = None
        if self.auth_user is not None and self.auth_pwd is not None:
            self._auth_data = requests.auth.HTTPBasicAuth(
                self.auth_user, self.auth_pwd)

        self._base_uri = 'http{secured}://{host}{sep_port}{port}/'.format(
            secured='s' if self.is_secured else '',
            host=self.host,
            sep_port=':' if self.port is not None else '',
            port=self.port if self.port is not None else '')

    def __repr__(self):
        return (
            '<{self.__class__.__name__}>('
            'host="{self.host}"'
            ', port={self.port}'
            ', is_secured={self.is_secured}'
            ', auth_user={self.auth_user}'
            ')'.format(self=self))

    def _get(self, uri, *, use_auth=True, expected_status=(200,),
             not_found_raise_exc=DatasetNotFoundError, **kwargs):
        """Execute a GET request.

        :param str uri: Request's URI to send.
        :param bool use_auth: If True, use BASIC authentication (default True).
        :param tuple(int) expected_status:
            Expected response status codes (default 200).
        :param Exception not_found_raise_exc:
            Exception raised on 404 response status code.
        :returns requests.Response:
            The HTTP response received after sending request.
        :raises FusekiClientError:
        :raises FusekiClientResponseError:
        :raises DatasetNotFoundError:
        """
        # prepare request authentication params
        auth_data = self._auth_data if use_auth else None
        try:
            # send request
            raw_response = requests.get(uri, auth=auth_data, **kwargs)
            if raw_response.status_code == 404:
                raise not_found_raise_exc(raw_response.reason)
            if raw_response.status_code not in expected_status:
                raise FusekiClientResponseError(raw_response.reason)
        except requests.exceptions.ConnectionError as exc:
            raise FusekiClientError(str(exc))
        return raw_response

    def _post(self, uri, *, use_auth=True, expected_status=(200,), **kwargs):
        """Execute a POST request.

        :param str uri: Request's URI to send.
        :param bool use_auth: If True, use BASIC authentication (default True).
        :param tuple(int) expected_status:
            Expected response status codes (default 200).
        :returns requests.Response:
            The HTTP response received after sending request.
        :raises FusekiClientError:
        :raises FusekiClientResponseError:
        :raises DatasetNotFoundError:
        """
        # prepare request authentication params
        auth_data = self._auth_data if use_auth else None
        try:
            # send request
            raw_response = requests.post(uri, auth=auth_data, **kwargs)
            if raw_response.status_code == 404:
                raise DatasetNotFoundError(raw_response.reason)
            if raw_response.status_code not in expected_status:
                raise FusekiClientResponseError(raw_response.reason)
        except requests.exceptions.ConnectionError as exc:
            raise FusekiClientError(str(exc))
        return raw_response

    def _delete(self, uri, *, use_auth=True, expected_status=(200,), **kwargs):
        """Execute a delete request.

        :param str uri: Request's URI to send.
        :param bool use_auth: If True, use BASIC authentication (default True).
        :param tuple(int) expected_status:
            Expected response status codes (default 200).
        :returns requests.Response:
            The HTTP response received after sending request.
        :raises FusekiClientError:
        :raises FusekiClientResponseError:
        :raises DatasetNotFoundError:
        """
        # prepare request authentication params
        auth_data = self._auth_data if use_auth else None
        try:
            # send request
            raw_response = requests.delete(uri, auth=auth_data, **kwargs)
            if raw_response.status_code == 404:
                raise DatasetNotFoundError(raw_response.reason)
            if raw_response.status_code not in expected_status:
                raise FusekiClientResponseError(raw_response.reason)
        except requests.exceptions.ConnectionError as exc:
            raise FusekiClientError(str(exc))
        return raw_response
