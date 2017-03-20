#!/usr/bin/env python3
import argparse
from beehive import Beehive
import datetime
import requests
from decoders import decode_lines
from elasticsearch import Elasticsearch


parser = argparse.ArgumentParser()

parser.add_argument('--after')
parser.add_argument('--before')

args = parser.parse_args()

if args.after:
    after = datetime.date(*map(int, args.after.split('-')))
else:
    after = datetime.date.today()

if args.before:
    before = datetime.date(*map(int, args.before.split('-')))
else:
    before = None


beehive = Beehive('beehive1.mcs.anl.gov')
datasets = list(beehive.datasets(after=after, before=before))

es = Elasticsearch()

for number, dataset in enumerate(datasets):
    filename = 'data/{}-{}.txt'.format(dataset['node_id'], dataset['date'].strftime('%Y%m%d'))

    r = requests.get(dataset['url'])

    if r.status_code != 200:
        print('failed {} [{} of {}]'.format(filename, number + 1, len(datasets)))
        continue

    print('got {} [{} of {}]'.format(filename, number + 1, len(datasets)))

    body = decode_lines(dataset, map(bytes.decode, r.iter_lines()))

    if body:
        es.bulk(body)
