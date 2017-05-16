#!/usr/bin/env python3
import elasticsearch
import elasticsearch.helpers
from waggle.beehive import Beehive

locations = {
  '001e0610b9e7': {
    'address': '3501 S Western Blvd, Chicago, IL 60609, USA',
    'location': '41.8301715, -87.68425839999999',
  }
}


def actions():
    beehive = Beehive('beehive1.mcs.anl.gov')
    nodes = beehive.nodes()

    for node in nodes:
        doc = {
            '_op_type': 'index',
            '_index': 'waggle',
            '_type': 'node',
            '_id': node['node_id'],
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

        yield doc


es = elasticsearch.Elasticsearch()
elasticsearch.helpers.bulk(es, actions())
