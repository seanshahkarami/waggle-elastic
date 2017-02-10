#!/usr/bin/env python3
import os.path
import pika
import ssl
from urllib.parse import urlencode
from datetime import datetime
import sqlite3
import time


db = sqlite3.connect('/var/lib/monitoring/monitor.db')

c = db.cursor()
c.execute('CREATE TABLE IF NOT EXISTS updates (node TEXT, plugin TEXT, ts TIMESTAMP, PRIMARY KEY (node, plugin) ON CONFLICT REPLACE)')
db.commit()


url = 'amqps://node:waggle@localhost:23181?{}'.format(urlencode({
    'ssl': 't',
    'ssl_options': {
        'certfile': '/mnt/waggle/SSL/node/cert.pem',
        'keyfile': '/mnt/waggle/SSL/node/key.pem',
        'ca_certs': '/mnt/waggle/SSL/waggleca/cacert.pem',
        'cert_reqs': ssl.CERT_REQUIRED
    }
}))

connection = pika.BlockingConnection(pika.URLParameters(url))

channel = connection.channel()

channel.queue_declare(queue='monitor-data', durable=True)
channel.queue_bind(queue='monitor-data', exchange='data-pipeline-in', routing_key='')


def callback(ch, method, properties, body):
    dt = datetime.fromtimestamp(properties.timestamp // 1000)
    node_id = properties.reply_to[4:]
    plugin = properties.app_id
    print(node_id, plugin, dt.timestamp(), time.time(), flush=True)

    c = db.cursor()
    c.execute('INSERT INTO updates VALUES (?,?,?)', (node_id, plugin, time.time()))
    db.commit()


channel.basic_consume(callback, queue='monitor-data', no_ack=True)
channel.start_consuming()

