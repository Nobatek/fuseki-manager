from ..utils import url_validator
from ..exceptions import EmptyDBError, UniquenessDBError, ArgumentError
from .base import FusekiBaseClient


class FusekiSPARQLClient(FusekiBaseClient):
    """Fuseki 'sparql' API client (sparql service)."""

    def __init__(self, ds_name, *,
                 service_name='sparql', namespaces={},
                 **kwargs):

        super().__init__(**kwargs)
        self._namespaces = namespaces
        self._service_uri = self._build_uri(ds_name, service_name)

    def _build_uri(self, ds_name, service_name='sparql'):
        """Build service URI.

        :param str ds_name: Dataset's name used in URI.
        :returns str: Service's absolute URI.
        """
        return '{}{}/{}'.format(self._base_uri, ds_name, service_name)

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

    def _exec_query(self, prepared_query):
        params = {'query': prepared_query}
        response = self._get(self._service_uri, params=params)
        return response.json()

    def raw_query(self, query, **kwargs):
        query = self._prepare_query(query, **kwargs)
        return self._exec_query(query)

    def query(self, query, *,
              raise_if_empty=False, raise_if_many=False,
              **kwargs):
        """List SPARQL query results."""
        query = self._prepare_query(query, **kwargs)
        jsonres = self._exec_query(query)

        results = jsonres['results']['bindings']
        nb_results = len(results)

        if nb_results < 1:
            if raise_if_empty:
                raise EmptyDBError
            return None

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


def _parse_uri(uri, raise_if_not_uri=True):
    if isinstance(uri, str) and url_validator(uri):
        return '<{}>'.format(uri)
    if raise_if_not_uri:
        raise ArgumentError('Not a valid URI.')
    return uri
