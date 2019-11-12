"""Tests on Fuseki manager utils."""

import os
import io
import pytest

from fuseki_manager.utils import build_http_file_obj, url_validator
from fuseki_manager.exceptions import InvalidFileError


class TestFusekiManagerUtils():

    def test_utils_build_http_file_obj(self):

        file_path = os.path.realpath(__file__)
        file_obj = build_http_file_obj(file_path, 'application/rdf+xml')
        assert len(file_obj) == 3
        assert file_obj[0] == os.path.basename(file_path)
        assert isinstance(file_obj[1], io.BufferedReader)
        assert file_obj[2] == 'application/rdf+xml'

    def test_utils_build_http_file_obj_errors(self):

        with pytest.raises(InvalidFileError):
            build_http_file_obj('test', 'text/plain')

    def test_utils_url_validator(self):
        assert (url_validator('http://foobar.net'))
        assert (url_validator('https://foo-5-bar.net/baz/'))
        assert (url_validator('https://localhost:3030/baz/zed#'))
        assert (not url_validator('//foo_bar.net'))
        assert (not url_validator('//foà0@o§-bar.net'))
        assert (not url_validator('http://10.0.0.1'))
