#!/usr/bin/env python3
import json
from elasticsearch import Elasticsearch
from waggle.beehive import Beehive

locations = {
  '001e0610b9e7': {
    'address': '3501 S Western Blvd, Chicago, IL 60609, USA',
    'location': '41.8301715, -87.68425839999999',
  }
}

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
        'address': node['location'],
        'groups': node['groups'],
        'opmode': node['opmode'],
    }

    try:
        location = locations[node['node_id']]
        doc['address'] = location['address']
        doc['location'] = location['location']
    except KeyError:
        pass

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
