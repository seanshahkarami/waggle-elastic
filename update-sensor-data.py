#!/usr/bin/env python3
import argparse
import requests
from decoders import decode_lines
from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.ERROR)


parser = argparse.ArgumentParser()

parser.add_argument('--id')
parser.add_argument('--after')
parser.add_argument('--before')

args = parser.parse_args()

es = Elasticsearch()

q = {
    "size": 2500,
    "query": {
        "bool": {
            "must": []
        },
    },
}

if args.id:
    q['query']['bool']['must'].append({
        'wildcard': {
            'node_id': args.id
        }
    })

if args.after and args.before:
    q['query']['bool']['must'].append({
        'range': {
            'date': {
                'gte': args.after,
                'lt': args.before,
            }
        }
    })
elif args.after:
    q['query']['bool']['must'].append({
        'range': {
            'date': {
                'gte': args.after,
            }
        }
    })
elif args.before:
    q['query']['bool']['must'].append({
        'range': {
            'date': {
                'lt': args.before,
            }
        }
    })

results = es.search('datasets', 'dataset', q)
datasets = [r['_source'] for r in results['hits']['hits']]

for number, dataset in enumerate(datasets):
    index = 'data-{}-{}.txt'.format(dataset['node_id'], dataset['date'].replace('/', ''))

    r = requests.get(dataset['url'])

    if r.status_code != 200:
        print('failed {} [{} of {}]'.format(index, number + 1, len(datasets)))
        continue

    print('got {} [{} of {}]'.format(index, number + 1, len(datasets)))

    body = decode_lines(dataset, map(bytes.decode, r.iter_lines()))

    if body:
        es.bulk(body)
