=======================
Elasticsearch Connector
=======================

.. contents::
    :local:
    :backlinks: none
    :depth: 1

Overview
--------

The Elasticsearch Connector for Presto allows access to Elasticsearch data from Presto. This tutorial shows how to set up the Elasticsearch Connector to run SQL on Elasticsearch.

.. note::

    It is highly recommend to use Elasticsearch 5.1.2 or later.

Configuration
-------------

To configure the Elasticsearch connector, create a catalog properties file
``etc/catalog/elasticsearch.properties`` with the following contents,
replacing the properties as appropriate:

.. code-block:: none

    connector.name=elasticsearch
    elasticsearch.table-names=schema.table1,schema.table2
    elasticsearch.default-schema=default
    elasticsearch.table-description-dir=etc/elasticsearch/
    elasticsearch.scroll-size=1000
    elasticsearch.scroll-time=60000


Multiple Elasticsearch Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Elasticsearch clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``users.properties``, Presto
will create a catalog named ``users`` using the configured connector.

Configuration Properties
------------------------

The following configuration properties are available:

======================================= ==============================================================================
Property Name                           Description
======================================= ==============================================================================
``elasticsearch.table-names``           List of all tables provided by the catalog
``elasticsearch.default-schema``        Default schema name for tables
``elasticsearch.table-description-dir`` Directory containing topic description files
``elasticsearch.scroll-size``           Maximum number of hits to be returned with each batch of Elasticsearch scroll
``elasticsearch.scroll-time``           Number of milliseconds the Elasticsearch would keep live for a search context
======================================= ==============================================================================

``elasticsearch.table-names``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Comma-separated list of all tables provided by this catalog. A table name
can be unqualified (simple name) and will be put into the default schema
(see below) or qualified with a schema name (``<schema-name>.<table-name>``).

For each table defined here, a table description file (see below) must exist.

This property is required; there is no default and at least one table must be defined.

``elasticsearch.default-schema``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Defines the schema which will contain all tables that were defined without
a qualifying schema name.

This property is optional; the default is ``default``.

``elasticsearch.table-description-dir``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

References a folder within Presto deployment that holds one or more JSON
files (must end with ``.json``) which contain table description files.

This property is optional; the default is ``etc/elasticsearch``.

``elasticsearch.scroll-size``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
use scroll and fetch to get data from Elasticsearch. This property defines the maximum
number of hits to be returned with each batch of Elasticsearch scroll.

This property is optional; the default is ``1000``.

``elasticsearch.scroll-time``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Presto connects to Elasticsearch using an Elasticsearch client, the client will
use scroll and fetch to get data from Elasticsearch. This property defines number of
milliseconds the Elasticsearch client would keep live for a search context

This property is optional; the default is ``60000``.

Table Definition Files
----------------------

Elasticsearch maintains documentations and indexes in a highly scalable way. It provides
full-text search and analytics functionalities. For Presto, Elasticsearch data must be
mapped into tables and columns to allow queries against the data.

A table definition file consists of a JSON definition for a table. The
name of the file can be arbitrary but must end in ``.json``.

.. code-block:: none

    {
        "tableName": ...,
        "schemaName": ...,
        "hostAddress": ...,
        "port": ...,
        "clusterName": ...,
        "index": ...,
        "type": ...
    }

=============== ========= ============== =============================
Field           Required  Type           Description
=============== ========= ============== =============================
``tableName``   required  string         Presto table name defined by this file.
``schemaName``  optional  string         Schema which will contain the table. If omitted, the default schema name is used.
``hostAddress`` required  string         Elasticsearch Search Node host address.
``port``        required  string         Elasticsearch Search Node port number.
``clusterName`` required  string         Elasticsearch cluster name.
``index``       required  string         Elasticsearch index that is backing this table.
``type``        required  string         Elasticsearch type, which is the class of object that the document represents.
=============== ========= ============== =============================
