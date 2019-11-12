"""Jena/Fuseki admin API client to manage server and datasets throught HTTP"""

from dateutil import parser as dt_parser

from .base import FusekiBaseClient
from .data import FusekiDataClient

from ..utils import build_http_file_obj
from ..exceptions import (
    DatasetAlreadyExistsError,
    TaskNotFoundError)


class FusekiAdminClient(FusekiBaseClient):
    """Fuseki 'administration' API client (administration service)."""

    def __init__(self, *, host='localhost', port=3030, is_secured=False,
                 user=None, pwd=None):
        """
        :param str host: Fuseki host name. (default 'localhost')
        :param int port: Port used by Fuseki instance. (default 3030)
        :param bool is_secured:
            Should secured channel be used (https)? (default False)
        :param str user: User name used in BASIC authentication.
        :param str pwd: Password for BASIC authentication.
        """
        super().__init__(
            host=host, port=port, is_secured=is_secured, user=user, pwd=pwd)

        self._service_data = FusekiDataClient(
            host=host, port=port, is_secured=is_secured, user=user, pwd=pwd)

    def _build_uri(self, service_name):
        """Build service URI.

        :param str service_name: Service name used in URI.
        :returns str: Service's absolute URI.
        """
        return '{}$/{}'.format(self._base_uri, service_name)

    def ping(self):
        """A guaranteed low cost endpoint to test whether a server
        is running or not.

        :returns datetime: Date and time (UTC) of the server check.
        """
        uri = self._build_uri('ping')
        response = self._get(uri, use_auth=False)
        # TODO: remove python-dateutil dependency
        # Starting from Python 3.7, strptime supports colon delimiters
        # in UTC offsets (https://stackoverflow.com/a/48539157)
        return dt_parser.parse(response.text)

    def server_info(self):
        """Get details about the server and it's current status.

        :returns dict: JSON format.
        """
        uri = self._build_uri('server')
        response = self._get(uri)
        return response.json()

    def get_all_datasets(self):
        """Get a container representing all datasets present in the server.

        :returns dict: JSON format.
        """
        uri = self._build_uri('datasets')
        response = self._get(uri)
        return response.json()

    def create_dataset(self, ds_name, *, ds_type='mem'):
        """Add a dataset to a running server.

        :param str ds_name: Dataset's name.
        :param str ds_type: Dataset's type (either 'mem' or 'tdb').
        :returns dict: Details on dataset container created, JSON format.
        """
        if ds_type not in ('mem', 'tdb',):
            raise ValueError('Invalid dbType: {}'.format(ds_type))
        uri = self._build_uri('datasets')
        query_params = {'dbType': ds_type, 'dbName': ds_name}
        response = self._post(
            uri, params=query_params, expected_status=(200, 409,))
        if response.status_code == 409:
            raise DatasetAlreadyExistsError(response.reason)
        return self.get_dataset(ds_name)

    def create_dataset_from_config_file(self, config_path):
        """Sets up a dataset from a configuration file on a running server.

        :param str|Path config_path: Configuration file path (turtle format).
        :returns bool: True if no errors raised.
        """
        uri = self._build_uri('datasets')
        # build files parameter
        config_path = ('file', build_http_file_obj(config_path, 'text/turtle'))
        response = self._post(
            uri, expected_status=(200, 409,), files=[config_path])
        if response.status_code == 409:
            raise DatasetAlreadyExistsError(response.reason)
        return True

    def get_dataset(self, ds_name):
        """Get a dataset can from a running server.

        :param str ds_name: Dataset's name.
        :returns dict: Details on dataset container, JSON format.
        """
        service_name = 'datasets/{}'.format(ds_name)
        uri = self._build_uri(service_name)
        response = self._get(uri)
        return response.json()

    def delete_dataset(self, ds_name, *, force_drop_data=False):
        """The dataset name and the details of its configuration are completely
        deleted and can not be recovered.

        ..Note:
            By default, the data of a TDB dataset is not deleted by
            administration service.
            To delete dataset's data, Fuseki data service API can be involved
            by setting 'force_drop_data' parameter to True.

        :param str ds_name: The name of the dataset to remove.
        :param bool force_drop_data:
            Execute a 'DROP ALL' query before removing dataset. (default False)
        :returns bool: True if deleted without errors.
        """
        # data of a 'TDB' dataset is not removed, so execute a 'DROP ALL' query
        # https://jena.apache.org/documentation/fuseki2/fuseki-server-protocol.html#removing-a-dataset
        if force_drop_data:
            self._service_data.drop_all(ds_name)

        # remove dataset
        service_name = 'datasets/{}'.format(ds_name)
        uri = self._build_uri(service_name)
        self._delete(uri)

        return True

    def set_dataset_state(self, ds_name, *, state='active'):
        """Set a dataset's status to 'active' or 'offline' on a running server.

        :param str ds_name: Dataset's name.
        :param str state: Dataset's new state, either 'active' or 'offline'.
        :returns bool:
            True if state is succesfully updated to 'active',
            False if state is successfully updated to 'offline'.
        """
        if state not in ('active', 'offline',):
            raise ValueError('Invalid state value: {}'.format(state))
        service_name = 'datasets/{}'.format(ds_name)
        uri = self._build_uri(service_name)
        query_params = {'state': state}
        self._post(uri, params=query_params)
        return True if state == 'active' else False

    def get_all_stats(self):
        """Get statistics all datasets in a single response.

        :returns dict: Details on all datasets' statistics, JSON format.
        """
        uri = self._build_uri('stats')
        response = self._get(uri)
        return response.json()

    def get_stats(self, ds_name):
        """Get statistics for a defined dataset in a single response.

        :param str ds_name: Dataset's name.
        :returns dict: Details on a dataset's statistics, JSON format.
        """
        service_name = 'stats/{}'.format(ds_name)
        uri = self._build_uri(service_name)
        response = self._get(uri)
        return response.json()

    def get_all_backups(self):
        """Returns a list of all the files in the backup area of the server.
        This is useful for managing the files externally.

        :returns dict: Details on dataset container, JSON format.
        """
        uri = self._build_uri('backups-list')
        response = self._get(uri)
        return response.json()

    def create_backup(self, ds_name):
        """This operation initiates a backup and returns a JSON object with
        the task Id in it.
        Backups are written to the server local directory 'backups' as
        gzip-compressed N-Quads files.

        :param str ds_name: Dataset's name.
        :returns dict: Details on backup task created, JSON format.
        """
        service_name = 'backup/{}'.format(ds_name)
        uri = self._build_uri(service_name)
        response = self._post(uri)
        return response.json()

    def get_all_tasks(self):
        """Returns a description of all running and recently tasks. A finished
        task can be identified by having a "finishPoint" field.

        ..Note:
            Some operations cause a background task to be executed, backup is
            an example. The result of such operations includes a json object
            with the task id and also a Location: header with the URL of the
            task created.
            Details of the last few completed tasks are retained, up to a fixed
            number. The records will eventually be removed as later tasks
            complete, and the task URL will then return 404.

        :returns dict:
            Details on all running and recently finished tasks, JSON format.
        """
        uri = self._build_uri('tasks')
        response = self._get(uri)
        return response.json()

    def get_task(self, task_id):
        """Get a description about one single task.
        Thus the progress of a task can be monitored.

        :param int task_id: ID of an asynchronous task.
        :returns dict: Details on a task, JSON format.
        """
        service_name = 'tasks/{}'.format(task_id)
        uri = self._build_uri(service_name)
        response = self._get(uri, not_found_raise_exc=TaskNotFoundError)
        return response.json()

    def restore_data(self, ds_name, file_paths):
        """Upload and insert datas by sending a list of files to a dataset.
        (Fuseki data service is involved.)

        :param str ds_name: Dataset's name.
        :param list[Path] file_paths: List of file's path to send.
        :returns dict: Details on data inserted, JSON format.
        """
        return self._service_data.upload_files(ds_name, file_paths)
