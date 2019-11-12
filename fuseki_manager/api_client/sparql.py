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
        namespaces = self._namespaces.copy().update(namespaces)
        if namespaces:
            pattern = "PREFIX {}: {} "
            ns_str = ''.join(_parse_uri(ns) for ns in namespaces.items())

        bind_str = ''
        if bindings:
            keys = ' '.join(map(lambda x: '?{}'.format(x), bindings.keys()))
            values = ' '.join(bindings.values())
            pattern = " VALUES ({k}){{({v})}}"
            bind_str = pattern.format(k=keys, v=values)

        prepared_query = '{}{}{}'.format(ns_str, query, bind_str)
        params = {'query': prepared_query}
        response = self._get(self._service_uri, params=params)
        return response.json()

    def query(self, query, *,
              raise_if_empty=False, raise_if_many=False,
              **kwargs):
        """List SPARQL query results."""
        jsonres = self._prepare_query(query, **kwargs)

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


def _parse_uri(uri):
    if isinstance(uri, str) and url_validator(uri):
        return '<{}>'.format(uri)
    raise ArgumentError('Not a valid URI.')
