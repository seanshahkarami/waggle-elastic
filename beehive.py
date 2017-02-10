import requests
import re


def get_nodes():
    r = requests.get('http://beehive1.mcs.anl.gov')
    assert r.status_code == 200

    for m in re.finditer('"(?P<url>http://beehive1.mcs.anl.gov/nodes/0000(?P<id>.*))"', r.text):
        yield m.groupdict()


def get_node_datasets(node):
    r = requests.get(node['url'])
    assert r.status_code == 200

    for m in re.finditer('"(?P<url>http.*date=(?P<date>\d{4}-\d{2}-\d{2}).*version=(?P<version>\d+|\d+raw))"', r.text):
        yield m.groupdict()


def get_datasets():
    for node in get_nodes():
        yield from get_node_datasets(node)


for dataset in filter(lambda d: d['version'] == '2raw', get_datasets()):
    print(dataset)
