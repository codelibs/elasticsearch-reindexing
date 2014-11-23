Elasticsearch Reindexing
=======================

## Overview

Elasticsearch Reindexing Plugin provides a feature to create a new index from an exsiting index.
If you want to add new analyzers or make changes to exsiting fields, you need to re-create your index.

## Version

| Version   | elasticsearch |
|:---------:|:-------------:|
| master    | 1.4.X         |
| 1.4.1     | 1.4.0         |
| 1.3.0     | 1.3.0         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-reindexing/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-reindexing/1.4.1

## Usage

### Run Reindexing

To re-index your index, send the following request:

    localhost:9200/{fromindex}/{fromtype}/_reindex/{toindex}/{totype}

fromtype and totype is optional.
If you want to create "newsample" index from "sample" index, run:

    $ curl -XPOST localhost:9200/sample/_reindex/newsample/
    {"acknowledged":true,"name":"8e0c3743-41ea-4268-aa81-d4c38058a407"}

A value of "name" is a reindexing name(ex. 8e0c3743-41ea-4268-aa81-d4c38058a407).
To wait for the reindexing process, use "wait\_for\_completion":

    $ curl -XPOST localhost:9200/sample/_reindex/newsample/?wait_for_completion=true

Sending reindexing data to a remote cluster, use "url":

    $ curl -XPOST localhost:9200/sample/_reindex/newsample/?url=http%3A%2F%2Flocalhost%3A9200%2F

### Check Reindexing process

Sending GET request, you can check current processes for reindexing:

    $ curl -XGET localhost:9200/_reindex

### Stop Reindexing process

To stop a reindexing process, send DELETE request by the reindexing name:

    $ curl -XDELETE localhost:9200/_reindex/{name}

For example,

    $ curl -XDELETE localhost:9200/_reindex/8e0c3743-41ea-4268-aa81-d4c38058a407

