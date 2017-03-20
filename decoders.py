#!/usr/bin/env python3
from datetime import datetime
import binascii
import re
import json


def normalize(x):
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return x
    if isinstance(x, str):
        return re.sub('\W+', '_', x).lower()
    if isinstance(x, tuple):
        return tuple(map(normalize, x))
    if isinstance(x, list):
        return list(map(normalize, x))
    if isinstance(x, dict):
        return dict((normalize(k), normalize(v)) for k, v in x.items())
    raise ValueError('cannot normalize {}'.format(x))


def decode_coresense_data(data):
    from waggle.coresense.utils import decode_frame
    return decode_frame(data)


def decode_alphasense_data(data):
    from alphasense.opc import decode18
    return decode18(data)


def decode_gps_data(data):
    return {'string': data.decode()}


decoders = {
    ('coresense', '3', 'frame'): decode_coresense_data,
    ('alphasense', '1', 'histogram'): decode_alphasense_data,
    ('gps', '1', 'gps'): decode_gps_data,
}


def decode_lines(dataset, lines):
    node_id = dataset['node_id']

    bulk = []

    for line in lines:
        doc = {}

        fields = line.strip().split(';')

        try:
            timestamp = datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            timestamp = datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S')

        doc['timestamp'] = timestamp.strftime('%Y/%m/%d %H:%M:%S')
        doc['node_id'] = node_id

        timestamp = timestamp
        plugin_name = fields[1]
        plugin_version = fields[2]
        key = fields[4]
        source = fields[5]

        doc['source'] = source

        try:
            data = binascii.unhexlify(source)
        except binascii.Error:
            continue

        try:
            decoder = decoders[(plugin_name, plugin_version, key)]
        except KeyError:
            continue

        try:
            doc['values'] = normalize(decoder(data))
        except AssertionError:
            continue

        index = {
            'index': {
                '_index': 'sensor',
                '_type': '{}-{}'.format(plugin_name, plugin_version),
                '_id': '{}'.format('-'.join([node_id, plugin_name, plugin_version, str(int(timestamp.timestamp()))])),
            }
        }

        bulk.append(json.dumps(index))
        bulk.append('\n')
        bulk.append(json.dumps(doc))
        bulk.append('\n')

    return ''.join(bulk)
