# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
import pika
import time
import logging
from haversine import haversine

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Wait for rabbitmq to come up
time.sleep(10)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Queues to consume and publish
channel.queue_declare(queue='', durable=True)
channel.queue_declare(queue='query3-pipe2', durable=True)

TARGET = "montreal"

STATION_NAME_IDX = 0
START_LATITUDE_IDX = 1
START_LONGITUDE_IDX = 2
END_LATITUDE_IDX = 3
END_LONGITUDE_IDX = 4
EOF = "#"

status = {}


def callback(ch, method, properties, body):
    """
        inpiut: [END_STATION, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
        output: [END_STATION, DISTANCE]
    """

    trip = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"action: filter_callback | result: done | Received EOF ")
        ch.basic_publish(exchange="", routing_key='query3-pipe2', body=encode(EOF))
        return

    start = (float(trip[START_LATITUDE_IDX]), float(trip[START_LONGITUDE_IDX]))
    end = (float(trip[END_LATITUDE_IDX]), float(trip[END_LONGITUDE_IDX]))
    distance = haversine(start, end)

    filtered = [trip[STATION_NAME_IDX], str(distance)]

    ch.basic_publish(exchange="", routing_key='query3-pipe2', body=encode(filtered))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success | msg: published => {filtered} ")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='query3-pipe1', on_message_callback=callback, auto_ack=False)

logging.info(
    f"action: calc_distance | result: in_progress | msg: start consuming from query3-pipe1 ")

channel.start_consuming()

logging.info(f"action: calc_distance | result: success ")
