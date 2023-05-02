# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
import pika
import time
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Wait for rabbitmq to come up
time.sleep(5)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='query3-pipe3', durable=True)
channel.queue_declare(queue='query3-pipe4', durable=True)

EOF = "#"

CITY_IDX = 0
STATION_IDX = 1
AVERAGE_IDX = 2

THRESHOLD = 6


def filter_station(ch, method, properties, body):
    """
        input:  [ CITY, STATION, AVERGE ]
        output: [ CITY, STATION ]
    """

    data = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {data} ")

    if data == EOF:
        logging.info(f"action: callback | result: done | msg: END OF FILE trips.")
        channel.basic_publish(exchange="station_topic",  routing_key='', body=data)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    if float(data[AVERAGE_IDX]) > THRESHOLD:
        result = [data[CITY_IDX], data[AVERAGE_IDX]]
        channel.basic_publish(exchange="",  routing_key='query3-pipe4', body=encode(result))
        status = "published"

    else:
        status = "ignored"

    logging.info(f"action: callback | result: in_progress | station: {data[STATION_IDX]} | average: {data[AVERAGE_IDX]} "
                 f"| status: {status} ")

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: callback | result: success ")


channel.basic_consume(queue="query3-pipe3", on_message_callback=filter_station, auto_ack=False)

logging.info(f"action: consuming | result: in_progress ")

channel.start_consuming()

logging.info(f"action: consuming | result: done")
