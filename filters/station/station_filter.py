from messaging_protocol import decode,encode
import pika
import time
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Wait for rabbitmq to come up
# time.sleep(2)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='raw_station_data', durable=True)
channel.exchange_declare(exchange='station_topic', exchange_type='fanout')


def filter_station(ch, method, properties, body):

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    filtered = reg # No filter applyed

    channel.basic_publish(exchange="station_topic",  routing_key='', body=encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


channel.basic_consume(queue="raw_station_data", on_message_callback=filter_station, auto_ack=False)

logging.info(f"action: consuming | result: in_progress ")

channel.start_consuming()

logging.info(f"action: consuming | result: done ")