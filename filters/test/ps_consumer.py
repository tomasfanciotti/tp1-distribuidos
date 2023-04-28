#!/usr/bin/env python3
import pika
import time
import os

# Wait for rabbitmq to come up
time.sleep(10)

TESTING_EXCHANGE = "weather_topic"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

result = channel.queue_declare(queue='', durable=True)
queue_name = result.method.queue
channel.queue_bind(exchange=TESTING_EXCHANGE, queue=queue_name)

def callback(ch, method, properties, body):
    print("Received {}".format(body))

channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()