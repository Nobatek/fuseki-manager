"""Jena/Fuseki admin API client to manage server and datasets throught HTTP.

Based on Fuseki server protocol:
https://jena.apache.org/documentation/fuseki2/fuseki-server-protocol.html
"""

from .admin import FusekiAdminClient    # noqa
from .data import FusekiDataClient      # noqa
from .base import FusekiBaseClient      # noqa
