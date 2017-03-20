#!/usr/bin/env python3
import json
from beehive import Beehive
from elasticsearch import Elasticsearch

beehive = Beehive('beehive1.mcs.anl.gov')
nodes = beehive.nodes()

bulk = []

for node in nodes:
    index = {
        'index': {
            '_index': 'waggle',
            '_type': 'node',
            '_id': node['node_id']
        }
    }

    doc = {
        'node_id': node['node_id'],
        'name': node['name'],
        'description': node['description'],
        'location': node['location'],
        'groups': node['groups'],
        'opmode': node['opmode'],
    }

    try:
        doc['reverse_ssh_port'] = node['reverse_ssh_port']
    except KeyError:
        pass

    bulk.append(json.dumps(index))
    bulk.append('\n')
    bulk.append(json.dumps(doc))
    bulk.append('\n')

body = ''.join(bulk)

es = Elasticsearch()
es.bulk(body)
