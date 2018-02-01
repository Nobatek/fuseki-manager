"""Tests on Fuseki manager utils."""

import os
import io
import pytest

from fuseki_manager.utils import build_rdf_file_obj
from fuseki_manager.exceptions import InvalidFileError


class TestFusekiManagerUtils():

    def test_utils_build_ref_file_obj(self):

        file_path = os.path.realpath(__file__)
        file_obj = build_rdf_file_obj(file_path)
        assert len(file_obj) == 3
        assert file_obj[0] == os.path.basename(file_path)
        assert isinstance(file_obj[1], io.BufferedReader)
        assert file_obj[2] == 'application/rdf+xml'

    def test_utils_build_ref_file_obj_errors(self):

        with pytest.raises(InvalidFileError):
            build_rdf_file_obj('test')
