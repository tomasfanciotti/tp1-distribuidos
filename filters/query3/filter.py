# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController           # module provided on the container
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "q3-filter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: filter | result: startup | node_id: {NODE_ID}")

CITY_IDX = 0
STATION_IDX = 1
AVERAGE_IDX = 2

THRESHOLD = 1


def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def filter_station(ch, method, properties, body):
    """
        input:  [ CITY, STATION, AVERAGE ]
        output: [ CITY, STATION, AVERAGE ]
    """

    data = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {data} ")

    if float(data[AVERAGE_IDX]) > THRESHOLD:
        rabbit.publish_queue('query3-pipe4', encode(data))
        status = "published"
    else:
        status = "ignored"

    logging.debug(f"action: callback | result: in_progress | station: {data[STATION_IDX]} | average: {data[AVERAGE_IDX]} "
                 f"| status: {status} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(f"action: callback | result: success ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)

logging.info(f"consuming query3-pipe3 | result: in_progress ")
rabbit.consume_queue("query3-pipe3", filter_station)

logging.info(f"action: consuming | result: done")
