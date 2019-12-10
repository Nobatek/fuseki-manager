"""Tests on Fuseki manager utils."""

import os
import io
import pytest

from fuseki_manager.utils import build_http_file_obj, is_url
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

    def test_utils_is_url(self):
        assert (is_url('http://foobar.net'))
        assert (is_url('https://foo-5-bar.net/baz/'))
        assert (is_url('https://localhost:3030/baz/zed#'))
        assert (is_url('<https://foo-5-bar.net/baz/>'))
        assert (not is_url('//foo_bar.net'))
        assert (not is_url('//foà0@o§-bar.net'))
        assert (not is_url('http://10.0.0.1'))
        assert (not is_url('<":">'))
