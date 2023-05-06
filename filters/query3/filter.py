# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
import pika
import time
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

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
    logging.debug(f"action: filter_callback | result: in_progress | body: {data} ")

    if data == EOF:
        logging.info(f"action: callback | result: done | msg: END OF FILE trips.")
        rabbit.publish_queue('query3-pipe4', encode(data))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if float(data[AVERAGE_IDX]) > THRESHOLD:
        result = [data[CITY_IDX], data[AVERAGE_IDX]]
        rabbit.publish_queue('query3-pipe4', encode(result))
        status = "published"

    else:
        status = "ignored"

    logging.debug(f"action: callback | result: in_progress | station: {data[STATION_IDX]} | average: {data[AVERAGE_IDX]} "
                 f"| status: {status} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(f"action: callback | result: success ")


rabbit = RabbitInterface()

logging.info(f"consuming query3-pipe3 | result: in_progress ")
rabbit.consume_queue("query3-pipe3", filter_station)

logging.info(f"action: consuming | result: done")
