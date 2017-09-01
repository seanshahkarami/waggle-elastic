#!/usr/bin/env python3
import elasticsearch
import elasticsearch.helpers
from waggle.beehive import Beehive
import requests
from datetime import datetime

r = requests.get('http://beehive1.mcs.anl.gov/api/1/nodes_last_ssh/')
last_ssh = dict((k[-12:], datetime.fromtimestamp(v / 1000).strftime('%Y/%m/%d %H:%M:%S')) for k, v in r.json().items())

r = requests.get('http://beehive1.mcs.anl.gov/api/1/nodes_last_data/')
last_data = dict((k[-12:], datetime.fromtimestamp(v / 1000).strftime('%Y/%m/%d %H:%M:%S')) for k, v in r.json().items())

r = requests.get('http://beehive1.mcs.anl.gov/api/1/nodes_last_log/')
last_log = dict((k[-12:], datetime.fromtimestamp(v / 1000).strftime('%Y/%m/%d %H:%M:%S')) for k, v in r.json().items())

locations = {
  '001e0610b9e7': {
    'address': '3501 S Western Blvd, Chicago, IL 60609, USA',
    'location': {'lat': 41.830, 'lon': -87.684},
  }
}

beehive = Beehive('beehive1.mcs.anl.gov')
nodes = beehive.nodes()

now = datetime.now()


def actions():
    for node in nodes:
        doc = {
            'node_id': node['node_id'],
            'name': node['name'],
            'description': node.get('description', ''),
            'address': node.get('location', ''),
            'groups': node['groups'],
            'opmode': node['opmode'],
        }

        try:
            location = locations[node['node_id']]
            doc['address'] = location['address']
            doc['location'] = location['location']
        except KeyError:
            pass

        doc['reverse_ssh_port'] = node.get('reverse_ssh_port', 0)

        doc['last_data'] = last_data.get(node['node_id'], '2000/01/01 00:00:00')
        doc['last_ssh'] = last_ssh.get(node['node_id'], '2000/01/01 00:00:00')
        doc['last_log'] = last_log.get(node['node_id'], '2000/01/01 00:00:00')

        doc['seconds_since_data'] = (now - datetime.strptime(doc['last_data'], '%Y/%m/%d %H:%M:%S')).total_seconds()
        doc['seconds_since_ssh'] = (now - datetime.strptime(doc['last_ssh'], '%Y/%m/%d %H:%M:%S')).total_seconds()
        doc['seconds_since_log'] = (now - datetime.strptime(doc['last_log'], '%Y/%m/%d %H:%M:%S')).total_seconds()

        yield {
            '_op_type': 'update',
            '_index': 'nodes',
            '_type': 'node',
            '_id': node['node_id'],
            'doc': doc,
            'doc_as_upsert': True,
        }


es = elasticsearch.Elasticsearch()
elasticsearch.helpers.bulk(es, actions())
