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


r = requests.get('http://beehive1.mcs.anl.gov/api/datasets?version=2raw')

if r.status_code != 200:
    raise RuntimeError('could not get dataset list')

datasets = r.json()

for dataset in datasets:
    dataset['date'] = datetime.strptime(dataset['date'], '%Y-%m-%d')

start = datetime(2017, 1, 1)
end = datetime(2017, 1, 1)

# can we do a query delete?
# es.delete('data-20170101')

for dataset in filter(lambda dataset: start <= dataset['date'] <= end, datasets):
    r = requests.get(dataset['url'])

    if r.status_code != 200:
        print('skipping dataset: {}'.format(dataset['url']))
        continue

    print('adding dataset: {}'.format(dataset['url']))

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
            'node_id': dataset['node_id'].lower()[-12:],
            'timestamp': timestamp.strftime('%Y/%m/%d %H:%M:%S'),
            'raw': b64encode(raw).decode(),
            'sensors': sensors,
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

        es.index(index='data-{:04}{:02}{:02}'.format(timestamp.year, timestamp.month, timestamp.day),
                 doc_type='coresense',
                 body=data)
