from ..utils import is_url, parse_url
from ..exceptions import EmptyDBError, UniquenessDBError, ArgumentError

from .base import FusekiBaseClient
from .data import FusekiDataClient


class FusekiSPARQLClient(FusekiBaseClient):
    """
    Fuseki 'sparql' API client (sparql service).

    :param ds_name: string - name of dataset to use
    :param query_service: string - name of service to use for selecting data
    :param update_service: string - name of service to use for updating data
    :param namespaces: dict - namespaces used as PREFIX for queries.
    Ex PREFIX key: <value>
    """

    def __init__(self, ds_name, *,
                 query_service='sparql', update_service='update',
                 namespaces={},
                 **kwargs):

        super().__init__(**kwargs)
        self._service_data = FusekiDataClient(**kwargs)

        self._ds_name = ds_name
        self._namespaces = namespaces
        self._query_service = query_service
        self._update_service = update_service

    def _build_uri(self, service):
        """Build service URI.

        :returns str: Service's absolute URI.
        """
        return '{}{}/{}'.format(
            self._base_uri,
            self._ds_name,
            service
        )

    def _prepare_query(self, query, namespaces={}, bindings={}):
        """Prepare query"""
        ns_str = ''
        ns = self._namespaces.copy()
        ns.update(namespaces)
        if ns:
            pattern = "PREFIX {}: {} "
            ns_str = ''.join(
                pattern.format(k, _parse_uri(v))
                for k, v in ns.items()
            )

        bind_str = ''
        if bindings:
            keys = ' '.join(map(lambda x: '?{}'.format(x), bindings.keys()))
            values = ' '.join(_parse_uri(x, False) for x in bindings.values())
            pattern = " VALUES ({k}) {{({v})}}"
            bind_str = pattern.format(k=keys, v=values)

        return '{}{}{}'.format(ns_str, query, bind_str)

    def update_query(self, query, **kwargs):
        """
        Execute query with 'update_service' endpoint.  Return raw HTTP response
        from Fuseki instance. This method is to use with INSERT queries.
        """
        prepared_query = self._prepare_query(query, **kwargs)
        params = {'update': prepared_query}
        uri = self._build_uri(self._update_service)
        return self._post(uri, data=params)

    def _exec_query(self, prepared_query):
        params = {'query': prepared_query}
        uri = self._build_uri(self._query_service)
        response = self._get(uri, params=params)
        return response.json()

    def raw_query(self, query, **kwargs):
        """
        Execute query with 'query_service' endpoint. Return raw JSON
        response from Fuseki instance. This method is to use with ASK queries.
        """
        query = self._prepare_query(query, **kwargs)
        return self._exec_query(query)

    def query(self, query, *,
              raise_if_empty=False, raise_if_many=False, **kwargs):
        """
        Execute query with 'query_service' endpoint. Apply post-treatment to
        Fuseki JSON response. Check result number and return only results
        bindings. This method is to use with SELECT queries.
        """
        query = self._prepare_query(query, **kwargs)
        jsonres = self._exec_query(query)

        results = jsonres['results']['bindings']
        nb_results = len(results)

        if nb_results < 1:
            if raise_if_empty:
                raise EmptyDBError
            return []

        if nb_results == 1:
            return results

        if nb_results > 1:
            if raise_if_many:
                raise UniquenessDBError
            return results

    def triples(self, sbj=None, pred=None, obj=None, **kwargs):
        """Generator over the triple store.
        Return triples that match the given pattern."""

        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        rawbinds = dict(s=sbj, p=pred, o=obj)
        bindings = {k: v for k, v in rawbinds.items() if v is not None}
        return [
            (r['s']['value'], r['p']['value'], r['o']['value'])
            for r in self.query(query, bindings=bindings, **kwargs)
        ]

    def value(self, sbj=None, pred=None, obj=None, raise_if_empty=True):
        """Get a value for a pair of two criteria."""

        if pred is not None and obj is not None:
            s, _, _ = self.triples(
                sbj=sbj, pred=pred, obj=obj,
                raise_if_empty=raise_if_empty, raise_if_many=True)[0]
            return s
        if sbj is not None and obj is not None:
            _, p, _ = self.triples(
                sbj=sbj, pred=pred, obj=obj,
                raise_if_empty=raise_if_empty, raise_if_many=True)[0]
            return p
        if sbj is not None and pred is not None:
            _, _, o = self.triples(
                sbj=sbj, pred=pred, obj=obj,
                raise_if_empty=raise_if_empty, raise_if_many=True)[0]
            return o

        msg = "Invalid arguments ({}, {}, {})"
        raise ArgumentError(msg.format(sbj, pred, obj))

    def upload_data(self, files, src_mime_type=None):
        """Upload and insert datas by sending a list of files to a dataset.
        (Fuseki data service is involved.)

        :param list[] files: List of file's to send.
        :returns dict: Details on data inserted, JSON format.

        Note: files could be a list of:
        - Path or string to file_name
        - file-like object
        """
        return self._service_data.upload_files(
            self._ds_name, files, src_mime_type
        )


def _parse_uri(value, raise_if_not_uri=True):
    if isinstance(value, str):
        value = str(value)
        if is_url(value):
            return parse_url(value)
        if not raise_if_not_uri:
            return value
    msg = 'Invalid URI [{}]'
    raise ArgumentError(msg.format(value))
