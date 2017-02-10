import requests
import re
from binascii import unhexlify
from base64 import b64encode
from waggle.coresense.utils import decode_frame
from datetime import datetime
from elasticsearch import Elasticsearch


es = Elasticsearch()


def normalize(s):
    return '_'.join(re.findall('\w+', s.lower()))


def normalize_keys(d):
    return dict((normalize(k), v) for k, v in d.items())


nodes = [
    '0000001e0610b9fd',
    '0000001e0610ba60',
    '0000001e0610b9eb',
    '0000001e0610bbe9',
    '0000001e0610b9e1',
]

for node_id in nodes:
    r = requests.get('http://beehive1.mcs.anl.gov/nodes/{}'.format(node_id))
    assert r.status_code == 200

    urls = re.findall('(http.*version=2raw)', r.text)

    for url in urls:
        r = requests.get(url)
        assert r.status_code == 200

        for line in map(bytes.decode, r.iter_lines()):
            row = line.strip().split(';')

            try:
                timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                timestamp = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')

            plugin_name = row[1]
            plugin_version = row[2]
            plugin_instance = row[3]
            datatype = row[4]
            raw = unhexlify(row[5])

            if plugin_name != 'coresense' or plugin_version != '3':
                continue

            sensors = normalize_keys(decode_frame(raw))

            for k, vs in sensors.items():
                sensors[k] = normalize_keys(vs)

            data = {
                'sensors': sensors,
                'node_id': node_id.lower()[-12:],
                'timestamp': timestamp.strftime('%Y/%m/%d %H:%M:%S'),
                'raw': b64encode(raw).decode(),
            }

            try:
                data['coresense_id'] = sensors['coresense_id']['mac_address'].lower()
                del data['sensors']['coresense_id']
            except KeyError:
                print('invalid packet')

            try:
                data['chemsense_id'] = sensors['chemsense_id']['mac_address'].lower()
                del data['sensors']['chemsense_id']
            except KeyError:
                pass

            es.index(index='sensor-{:04}-{:02}'.format(timestamp.year, timestamp.month),
                     doc_type='coresense',
                     body=data)
