==============
fuseki-manager
==============

.. image:: https://img.shields.io/travis/Nobatek/fuseki-manager/master.svg
        :target: https://travis-ci.org/Nobatek/fuseki-manager
        :alt: Build status

.. image:: https://coveralls.io/repos/github/Nobatek/fuseki-manager/badge.svg?branch=master
        :target: https://coveralls.io/github/Nobatek/fuseki-manager/?branch=master
        :alt: Code coverage

.. image:: https://landscape.io/github/Nobatek/fuseki-manager/master/landscape.svg?style=flat
        :target: https://landscape.io/github/Nobatek/fuseki-manager/master
        :alt: Code Health

About
=====

Jena/Fuseki API clients to manage server and datasets throught HTTP.

Examples
========

.. code-block:: python

    from fuseki_manager import FusekiAdminClient

    client = FusekiAdminClient()

    # Get datasets
    result = client.get_all_datasets()
    # 'result' contains information about datasets

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
