# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
# noinspection PyUnresolvedReferences
from eof_controller import EOFController           # module provided on the container
# noinspection PyUnresolvedReferences
from batching import Batching
import logging
import os
from haversine import haversine

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)
# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "q3-distance"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: distance | result: startup | node_id: {NODE_ID}")

TARGET = "montreal"

CITY_IDX = 0
STATION_NAME_IDX = 1
START_LATITUDE_IDX = 2
START_LONGITUDE_IDX = 3
END_LATITUDE_IDX = 4
END_LONGITUDE_IDX = 5


def log_eof(ch, method, properties, body):
    batching.push_buffer()
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def callback(ch, method, properties, body):
    """
        inpiut: [ CITY, END_STATION, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
        output: [ CITY, END_STATION, DISTANCE]
    """

    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip[CITY_IDX] != TARGET:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.debug(f"action: filter_callback | result: success | Ignored trip from other city instead {TARGET}.")
        return

    start = (float(trip[START_LATITUDE_IDX]), float(trip[START_LONGITUDE_IDX]))
    end = (float(trip[END_LATITUDE_IDX]), float(trip[END_LONGITUDE_IDX]))
    distance = haversine(start, end)

    filtered = [trip[CITY_IDX], trip[STATION_NAME_IDX], str(distance)]

    batching.publish_batch_to_queue('query3-pipe2', encode(filtered))
    logging.debug(f"action: filter_callback | result: success | msg: published => {filtered} ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)
batching = Batching(rabbit)

logging.info(f"action: consuming | result: in_progress ")
batching.consume_batch_queue("query3-pipe1", callback)

rabbit.disconnect()
logging.info(f"action: consuming | result: done")