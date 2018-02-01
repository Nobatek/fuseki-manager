"""Fixtures for client testing."""

import pytest

from fuseki_manager import FusekiAdminClient, FusekiDataClient


@pytest.fixture()
def admin_client():
    """Return a client instance of administration API."""
    return FusekiAdminClient(host='fuseki.local', port=None)


@pytest.fixture()
def data_client():
    """Return a client instance of data API."""
    return FusekiDataClient(host='fuseki.local', port=None)


@pytest.fixture()
def ds_name():
    """Return a sample name of a dataset."""
    return 'ds_test'


@pytest.fixture()
def ds_data():
    """Return a sample of dataset details."""
    return {
        'ds.name': '/{}'.format(ds_name()),
        'ds.state': True,
        'ds.services': [
            {
                'srv.type': 'Update',
                'srv.description': 'SPARQL Update',
                'srv.endpoints': ['update'],
            },
            {
                'srv.type': 'Upload',
                'srv.description': 'File Upload',
                'srv.endpoints': ['upload'],
            },
            {
                'srv.type': 'Query',
                'srv.description': 'SPARQL Query',
                'srv.endpoints': ['query', 'sparql',],
            },
            {
                'srv.type': 'GSP_RW',
                'srv.description': 'Graph Store Protocol',
                'srv.endpoints': ['data'],
            },
            {
                'srv.type': 'Quads_RW',
                'srv.description': 'HTTP Quads',
                'srv.endpoints': [''],
            },
            {
                'srv.type': 'GSP_R',
                'srv.description': 'Graph Store Protocol (Read)',
                'srv.endpoints': ['get'],
            },
        ]
    }


@pytest.fixture()
def stat_data():
    """Return a sample of statistics details."""
    return {
        '/{}'.format(ds_name()): {
            'Requests': 13256,
            'RequestsGood': 13229,
            'RequestsBad': 27,
            'endpoints': {
                'update': {
                    'RequestsGood': 4007,
                    'RequestsBad': 0,
                    'Requests': 4007,
                    'operation': 'Update',
                    'description': 'SPARQL Update',
                },
                'upload': {
                    'RequestsGood': 0,
                    'RequestsBad': 1,
                    'Requests': 1,
                    'operation': 'Upload',
                    'description': 'File Upload',
                },
                'query': {
                    'RequestsGood': 9218,
                    'RequestsBad': 18,
                    'Requests': 9236,
                    'operation': 'Query',
                    'description': 'SPARQL Query',
                },
                'sparql': {
                    'RequestsGood': 0,
                    'RequestsBad': 0,
                    'Requests': 0,
                    'operation': 'Query',
                    'description': 'SPARQL Query',
                },
                'data': {
                    'RequestsGood': 4,
                    'RequestsBad': 8,
                    'Requests': 12,
                    'operation': 'GSP_RW',
                    'description': 'Graph Store Protocol',
                },
                '': {
                    'RequestsGood': 0,
                    'RequestsBad': 0,
                    'Requests': 0,
                    'operation': 'Quads_RW',
                    'description': 'HTTP Quads',
                },
                'get': {
                    'RequestsGood': 0,
                    'RequestsBad': 0,
                    'Requests': 0,
                    'operation': 'GSP_R',
                    'description': 'Graph Store Protocol (Read)',
                },
            },
        },
    }


@pytest.fixture()
def task_data():
    """Return a sample of task details."""
    return {
        'task': 'backup',
        'taskId': '1',
        'started': '2018-02-01T17:07:23.027+00:00',
        'finished': '2018-02-01T17:07:23.055+00:00',
    }
