#!/usr/bin/env python3
import argparse
import requests
from decoders import decode_lines
import elasticsearch
import elasticsearch.helpers


parser = argparse.ArgumentParser()
parser.add_argument('-i', '--id')
parser.add_argument('-a', '--after')
parser.add_argument('-b', '--before')
parser.add_argument('-r', '--refresh', action='store_true')
args = parser.parse_args()

es = elasticsearch.Elasticsearch()

q = {'query': {'bool': {'must': []}}}

if args.id:
    q['query']['bool']['must'].append({'wildcard': {'node_id': args.id}})

date_query = {}

if args.after:
    date_query['gte'] = args.after

if args.before:
    date_query['lt'] = args.before

if date_query:
    q['query']['bool']['must'].append({'range': {'date': date_query}})

if not args.refresh:
    q['query']['bool']['must_not'] = {
        'term': {'indexed': True}
    }

count = es.count('datasets', 'dataset', q)['count']
scan = elasticsearch.helpers.scan(es, index='datasets', doc_type='dataset', query=q)

for i, result in enumerate(scan):
    dataset = result['_source']

    index = 'data-{}-{}'.format(dataset['node_id'], dataset['date'].replace('/', ''))

    r = requests.get(dataset['url'])

    if r.status_code != 200:
        print('failed {} [{} of {}]'.format(index, i+1, count))
        continue

    print('added {} [{} of {}]'.format(index, i+1, count))

    body = decode_lines(dataset, map(bytes.decode, r.iter_lines()))

    if body:
        es.bulk(body)

    es.update(index='datasets', doc_type='dataset', id=result['_id'], body={
        'doc': {
            'indexed': True
        }
    })
