#!/usr/bin/env python3
import requests
from elasticsearch import Elasticsearch
import json


r = requests.get('http://beehive1.mcs.anl.gov/api/datasets')
datasets = r.json()

bulk = []

for dataset in datasets:

    index = {
        'index': {
            '_index': 'datasets',
            '_type': 'dataset',
            '_id': '{}-{}-{}'.format(dataset['node_id'], dataset['date'].replace('-', ''), dataset['version'])
        }
    }

    doc = {
        'date': dataset['date'].replace('-', '/'),
        'node_id': dataset['node_id'],
        'version': dataset['version'],
        'url': dataset['url'],
    }

    bulk.append(json.dumps(index))
    bulk.append('\n')
    bulk.append(json.dumps(doc))
    bulk.append('\n')

body = ''.join(bulk)

es = Elasticsearch()
es.bulk(body)
