CREATE OR REPLACE FUNCTION emit_rabbitmq()
RETURNS trigger AS $$

import pika
import json
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='musicbrainz', type='fanout', durable=True)

channel.basic_publish(exchange='musicbrainz',
                      routing_key='',
                      body=json.dumps({
                          'table': TD["table_name"],
                          'new': TD["new"],
                          'old': TD["old"],
                          'timestamp': str(time.time())
                      }))

connection.close()

$$ LANGUAGE 'plpythonu';
