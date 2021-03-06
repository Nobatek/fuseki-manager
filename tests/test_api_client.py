"""Tests on Fuseki clients."""

import datetime as dt
import os
import io
from pathlib import Path
import pytest
import responses

from fuseki_manager import FusekiAdminClient, FusekiDataClient
from fuseki_manager.api_client import FusekiBaseClient
from fuseki_manager import FusekiSPARQLClient
from fuseki_manager.api_client.sparql import _parse_uri

from fuseki_manager.exceptions import (
    # FusekiClientError,
    FusekiClientResponseError,
    DatasetAlreadyExistsError, DatasetNotFoundError,
    InvalidFileError, ArgumentError)


class TestFusekiBaseClient():

    def test_base_api_client(self):

        client = FusekiBaseClient()
        assert client.host == 'localhost'
        assert client.port == 3030
        assert not client.is_secured
        assert client.auth_user is None
        assert client.auth_pwd is None
        assert client._auth_data is None
        assert client._base_uri == 'http://localhost:3030/'

        client = FusekiBaseClient(
            host='fuseki.local', port=None, is_secured=True,
            user='admin', pwd='1234')
        assert client.host == 'fuseki.local'
        assert client.port is None
        assert client.is_secured
        assert client.auth_user == 'admin'
        assert client.auth_pwd == '1234'
        assert client._auth_data is not None
        assert client._base_uri == 'https://fuseki.local/'

    def test_base_api_client_repr(self):

        # A clear '__repr__' is nice!
        client = FusekiBaseClient()
        assert repr(client) == (
            '<FusekiBaseClient>('
            'host="localhost"'
            ', port=3030'
            ', is_secured=False'
            ', auth_user=None'
            ')')

    # @pytest.mark.slow
    # def test_base_api_client_errors(self, admin_client):
    #
    #     client = FusekiBaseClient(host='fuseki.local', port=None)
    #
    #     with pytest.raises(FusekiClientError):
    #         client._get(client._base_uri)
    #
    #     with pytest.raises(FusekiClientError):
    #         client._post(client._base_uri)
    #
    #     with pytest.raises(FusekiClientError):
    #         client._delete(client._base_uri)


class TestFusekiAdminClient():

    def test_admin_api_client_build_uri(self):

        client = FusekiAdminClient()
        uri = client._build_uri('admin_service')
        assert uri == 'http://localhost:3030/$/admin_service'

    @responses.activate
    def test_admin_api_client_ping(self, admin_client):

        response_data = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('ping'),
            status=200,
            body=response_data.isoformat(),
        )

        result = admin_client.ping()
        assert isinstance(result, dt.datetime)
        assert result == response_data

    @responses.activate
    def test_admin_api_client_server_info(self, admin_client, ds_data):

        response_data = {
            'version': '3.4.0',
            'built': '2017-07-17T11:43:07+0000',
            'startDateTime': '2018-01-11T09:56:19.145+00:00',
            'uptime': 1832221,
            'datasets': [ds_data],
        }
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('server'),
            status=200,
            json=response_data,
        )

        result = admin_client.server_info()
        assert result == response_data
        assert 'version' in result
        assert 'built' in result
        assert 'uptime' in result
        assert 'startDateTime' in result
        assert 'datasets' in result

    @responses.activate
    def test_admin_api_client_get_all_datasets(self, admin_client, ds_data):

        response_data = {
            'datasets': [ds_data],
        }
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('datasets'),
            status=200,
            json=response_data,
        )

        result = admin_client.get_all_datasets()
        assert result == response_data
        assert 'datasets' in result
        assert len(result['datasets']) == 1
        assert 'ds.name' in result['datasets'][0]
        assert 'ds.state' in result['datasets'][0]
        assert 'ds.services' in result['datasets'][0]
        assert len(result['datasets'][0]['ds.services']) > 0

    @responses.activate
    def test_admin_api_client_create_dataset(
            self, admin_client, ds_name, ds_data):

        responses.add(
            method=responses.POST,
            url=admin_client._build_uri('datasets'),
            status=200,
        )
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('datasets/{}'.format(ds_name)),
            status=200,
            json=ds_data,
        )

        result = admin_client.create_dataset(ds_name)
        assert result == ds_data
        assert 'ds.name' in result
        assert result['ds.name'] == '/{}'.format(ds_name)
        assert 'ds.state' in result
        assert 'ds.services' in result
        assert len(result['ds.services']) > 0

    @responses.activate
    def test_admin_api_client_create_dataset_from_config_file(
            self, admin_client, config_path):

        responses.add(
            method=responses.POST,
            url=admin_client._build_uri('datasets'),
            status=200,
        )

        result = admin_client.create_dataset_from_config_file(config_path)
        assert result

    def test_admin_api_client_create_dataset_errors(
            self, admin_client, ds_name, config_path):

        with pytest.raises(ValueError):
            admin_client.create_dataset(ds_name, ds_type='INVALID')

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.POST,
                url='{}?dbType=mem&dbName={}'.format(
                    admin_client._build_uri('datasets'), ds_name),
                status=409,
            )
            with pytest.raises(DatasetAlreadyExistsError):
                admin_client.create_dataset(ds_name)

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.POST,
                url=admin_client._build_uri('datasets'),
                status=409,
            )
            with pytest.raises(DatasetAlreadyExistsError):
                admin_client.create_dataset_from_config_file(config_path)

    @responses.activate
    def test_admin_api_client_get_dataset(
            self, admin_client, ds_name, ds_data):

        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('datasets/{}'.format(ds_name)),
            status=200,
            json=ds_data,
        )

        result = admin_client.get_dataset(ds_name)
        assert result == ds_data
        assert 'ds.name' in result
        assert result['ds.name'] == '/{}'.format(ds_name)
        assert 'ds.state' in result
        assert 'ds.services' in result
        assert len(result['ds.services']) > 0

    def test_admin_api_client_get_dataset_errors(self, admin_client, ds_name):

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.GET,
                url=admin_client._build_uri('datasets/NOT_FOUND'),
                status=404,
            )
            with pytest.raises(DatasetNotFoundError):
                admin_client.get_dataset('NOT_FOUND')

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.GET,
                url=admin_client._build_uri('datasets/{}'.format(ds_name)),
                status=401,
            )
            with pytest.raises(FusekiClientResponseError):
                admin_client.get_dataset(ds_name)

    def test_admin_api_client_delete_dataset(
            self, admin_client, ds_name, data_client):

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.DELETE,
                url=admin_client._build_uri('datasets/{}'.format(ds_name)),
                status=200,
            )

            result = admin_client.delete_dataset(ds_name)
            assert result

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.DELETE,
                url=admin_client._build_uri('datasets/{}'.format(ds_name)),
                status=200,
            )
            rsps.add(
                method=responses.POST,
                url='{}/update'.format(data_client._build_uri(ds_name)),
                status=200,
            )

            result = admin_client.delete_dataset(ds_name, force_drop_data=True)
            assert result

    def test_admin_api_client_set_dataset_state(self, admin_client, ds_name):

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.POST,
                url='{}/{}?state=offline'.format(
                    admin_client._build_uri('datasets'), ds_name),
                status=200,
            )
            result = admin_client.set_dataset_state(ds_name, state='offline')
            assert not result  # False: state set to offline

        with responses.RequestsMock() as rsps:
            rsps.add(
                method=responses.POST,
                url='{}/{}?state=active'.format(
                    admin_client._build_uri('datasets'), ds_name),
                status=200,
            )
            result = admin_client.set_dataset_state(ds_name, state='active')
            assert result  # True: state set to active

    def test_admin_api_client_set_dataset_state_errors(
            self, admin_client, ds_name):

        with pytest.raises(ValueError):
            admin_client.set_dataset_state(ds_name, state='INVALID')

    @responses.activate
    def test_admin_api_client_get_all_stats(self, admin_client, stat_data):

        response_data = {
            'datasets': stat_data,
        }
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('stats'),
            status=200,
            json=response_data,
        )

        result = admin_client.get_all_stats()
        assert result == response_data
        assert 'datasets' in result

    @responses.activate
    def test_admin_api_client_get_stats(
            self, admin_client, ds_name, stat_data):

        response_data = {
            'datasets': stat_data,
        }
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('stats/{}'.format(ds_name)),
            status=200,
            json=response_data,
        )

        result = admin_client.get_stats(ds_name)
        assert result == response_data
        assert 'datasets' in result
        assert '/{}'.format(ds_name) in result['datasets']

    @responses.activate
    def test_admin_api_client_get_all_backups(self, admin_client):

        response_data = {
            'backups': ["ds_test_2018-02-01_17-07-23.nq.gz"],
        }
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('backups-list'),
            status=200,
            json=response_data,
        )

        result = admin_client.get_all_backups()
        assert result == response_data
        assert 'backups' in result
        assert len(result['backups']) == 1

    @responses.activate
    def test_admin_api_client_create_backup(self, admin_client, ds_name):

        response_data = {'requestId': 14800, 'taskId': '1'}
        responses.add(
            method=responses.POST,
            url=admin_client._build_uri('backup/{}'.format(ds_name)),
            status=200,
            json=response_data,
        )

        result = admin_client.create_backup(ds_name)
        assert result == response_data
        assert 'taskId' in result
        assert 'requestId' in result

    @responses.activate
    def test_admin_api_client_get_all_tasks(self, admin_client, task_data):

        response_data = [task_data]
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('tasks'),
            status=200,
            json=response_data,
        )

        result = admin_client.get_all_tasks()
        assert result == response_data
        assert len(result) == 1
        assert result[0]['taskId'] == '1'

    @responses.activate
    def test_admin_api_client_get_task(self, admin_client, task_data):

        task_id = task_data['taskId']
        responses.add(
            method=responses.GET,
            url=admin_client._build_uri('tasks/{}'.format(task_id)),
            status=200,
            json=task_data,
        )

        result = admin_client.get_task(task_id)
        assert result == task_data
        assert 'taskId' in result
        assert result['taskId'] == '1'

    @responses.activate
    def test_admin_api_client_restore_data(
            self, admin_client, data_client, ds_name):

        response_data = {'count': 1163, 'tripleCount': 1163, 'quadCount': 0}
        responses.add(
            method=responses.POST,
            url=data_client._build_uri(ds_name, service_name='data'),
            status=200,
            json=response_data,
        )

        file_path = Path(os.path.realpath(__file__))
        result = admin_client.restore_data(ds_name, [file_path])
        assert result == response_data


class TestFusekiDataClient():

    def test_data_api_client_build_uri(self):

        client = FusekiDataClient()
        uri = client._build_uri('data_service')
        assert uri == 'http://localhost:3030/data_service'

    @responses.activate
    def test_data_api_client_drop_all(self, data_client, ds_name):

        responses.add(
            method=responses.POST,
            url='{}/update'.format(data_client._build_uri(ds_name)),
            status=200,
        )

        result = data_client.drop_all(ds_name)
        assert result

    @responses.activate
    def test_data_api_client_upload_files(self, data_client, ds_name):

        response_data = {'count': 1163, 'tripleCount': 1163, 'quadCount': 0}
        responses.add(
            method=responses.POST,
            url=data_client._build_uri(ds_name, service_name='data'),
            status=200,
            json=response_data,
        )

        file_path = Path(os.path.realpath(__file__))
        result = data_client.upload_files(ds_name, [file_path])
        assert result == response_data

        files = [io.BytesIO(b'demo'), io.BytesIO(b'test')]
        result = data_client.upload_files(ds_name, files)
        assert result == response_data

    def test_data_api_client_errors(self, data_client, ds_name):

        file_path = Path(os.path.realpath(__file__))

        with pytest.raises(TypeError):
            data_client.upload_files(ds_name, None)

        with pytest.raises(InvalidFileError):
            data_client.upload_files(ds_name, [file_path.parent])


class TestFusekiSPARQLClient():

    def test_sparql_api_client(self):
        ns = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'}
        client = FusekiSPARQLClient(
            ds_name='test', namespaces=ns, query_service='query'
        )

        assert client._ds_name == 'test'
        assert client._query_service == 'query'
        assert client._update_service == 'update'
        assert client._namespaces == ns
        uri = client._build_uri(client._query_service)
        assert uri == 'http://localhost:3030/test/query'

    def test_sparql_api_client_parse_uri(self):
        uri = _parse_uri('http://localhost:3030/data-service/query')
        assert uri == '<http://localhost:3030/data-service/query>'

        uri = _parse_uri('<http://localhost:3030/data-service/query>', False)
        assert uri == '<http://localhost:3030/data-service/query>'

        with pytest.raises(ArgumentError):
            _parse_uri('<RDFlib>[<http://localhost:3030/data-service/query>]')

        with pytest.raises(ArgumentError):
            _parse_uri({'this': 'is_not_a_string'}, False)

    def test_sparql_api_client_prepare_request(self, sparql_client):
        query = "SELECT ?s ?p ?o WHERE { ?s rdf:type ?var } LIMIT 25"
        bindings = {'var': 'http://www.foo.bar/baz'}
        pquery = sparql_client._prepare_query(query, bindings=bindings)
        assert pquery == "PREFIX rdf: <http://www.rdf.org/#> " \
            "SELECT ?s ?p ?o WHERE { ?s rdf:type ?var } LIMIT 25 " \
            "VALUES (?var) {(<http://www.foo.bar/baz>)}"

    @responses.activate
    def test_sparql_api_client_query(self, sparql_client, triple_data):
        responses.add(
            method=responses.GET,
            url=sparql_client._build_uri('sparql'),
            status=200,
            json=triple_data,
        )

        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 25"
        result = sparql_client.query(query)
        assert result == triple_data['results']['bindings']

    @responses.activate
    def test_sparql_api_client_triples(self, sparql_client, triple_data):
        expected_result = [
            (r['s']['value'], r['p']['value'], r['o']['value'])
            for r in triple_data['results']['bindings']
        ]

        responses.add(
            method=responses.GET,
            url=sparql_client._build_uri('sparql'),
            status=200,
            json=triple_data,
        )

        pred = 'rdf:type'
        result = sparql_client.triples(sbj=None, pred=pred, obj=None)
        assert result == expected_result

    @responses.activate
    def test_sparql_api_client_value(self, sparql_client, value_data):
        responses.add(
            method=responses.GET,
            url=sparql_client._build_uri('sparql'),
            status=200,
            json=value_data,
        )

        subj, pred = 'http://url.org/dummy#foo', 'rdf:type'
        result = sparql_client.value(sbj=subj, pred=pred)
        assert result == value_data['results']['bindings'][0]['o']['value']
