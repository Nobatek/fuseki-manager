==============
fuseki-manager
==============

.. image:: https://img.shields.io/travis/Nobatek/fuseki-manager/master.svg
        :target: https://travis-ci.org/Nobatek/fuseki-manager
        :alt: Build status

.. image:: https://coveralls.io/repos/github/Nobatek/fuseki-manager/badge.svg?branch=master
        :target: https://coveralls.io/github/Nobatek/fuseki-manager/?branch=master
        :alt: Code coverage

.. image:: https://api.codacy.com/project/badge/Grade/5b3d5c5fae194b1cb57891465182448a
        :target: https://www.codacy.com/app/lafrech/fuseki-manager
        :alt: Code health

About
=====

Jena/Fuseki API clients to manage server and datasets throught HTTP.
Based on `Fuseki server protocol <https://jena.apache.org/documentation/fuseki2/fuseki-server-protocol.html>`_.

Examples
========

.. code-block:: python

    from fuseki_manager import FusekiAdminClient

    client = FusekiAdminClient()

    # Get datasets
    result = client.get_all_datasets()
    # 'result' contains information about datasets


.. code-block:: python

    from fuseki_manager import FusekiSPARQLClient

    db = FusekiSPARQLClient('dataset_name')
    
    # SELECT request
    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 25"
    result = db.query(query)
    // result = JSON parsed response's results' bindings

    # Other kind of requests with results
    ns = {'foaf': 'http://xmlns.com/foaf/0.1/'}
    query = 'ASK  { ?x foaf:name  "Alice" }'
    result = db.raw_query(query, namespaces=ns)
    // result = Raw JSON parsed response

    # INSERT requets
    ns = {'dc': 'http://purl.org/dc/elements/1.1/'}
    query = 'INSERT DATA { <http://example/book1> dc:title "A new book" }'
    result = db.update_query(query, namespaces=ns)
    // result = Raw HTTP response


Installation
============

.. code-block:: shell

    pip install setup.py

Development
===========

**Use a virtual environnement to debug or develop**

.. code-block:: shell

    # Create virtual environment
    $ virtualenv -p /usr/bin/python3 $VIRTUALENVS_DIR/fuseki-manager

    # Activate virtualenv
    $ source $VIRTUALENVS_DIR/fuseki-manager/bin/activate

**Tests**

.. code-block:: shell

    # Install test dependencies
    $ pip install -e .[test]

    # Run tests
    $ py.test

    # Skip slow tests
    $ py.test -m 'not slow'

    # Run tests with coverage
    $ py.test --cov=fuseki_manager --cov-report term-missing
