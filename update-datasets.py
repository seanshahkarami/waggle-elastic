#!/usr/bin/env python3
import requests
import elasticsearch
import elasticsearch.helpers


r = requests.get('http://beehive1.mcs.anl.gov/api/datasets')
datasets = r.json()


def actions():
    for dataset in datasets:
        if dataset['version'] != '2raw':
            continue

        doc = {
            'date': dataset['date'].replace('-', '/'),
            'node_id': dataset['node_id'],
            'url': dataset['url'],
        }

        yield {
            '_op_type': 'update',
            '_index': 'datasets',
            '_type': 'dataset',
            '_id': '{}-{}'.format(dataset['node_id'], dataset['date'].replace('-', '')),
            'doc': doc,
            'doc_as_upsert': True,
        }


es = elasticsearch.Elasticsearch()
elasticsearch.helpers.bulk(es, actions())
