# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
import logging
from haversine import haversine

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

TARGET = "montreal"

CITY_IDX = 0
STATION_NAME_IDX = 1
START_LATITUDE_IDX = 2
START_LONGITUDE_IDX = 3
END_LATITUDE_IDX = 4
END_LONGITUDE_IDX = 5
EOF = "#"


def callback(ch, method, properties, body):
    """
        inpiut: [ CITY, END_STATION, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
        output: [ CITY, END_STATION, DISTANCE]
    """

    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"action: filter_callback | result: done | Received EOF ")
        rabbit.publish_queue('query3-pipe2', encode(EOF))
        return

    if trip[CITY_IDX] != TARGET:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.debug(f"action: filter_callback | result: success | Ignored trip from other city instead {TARGET}.")
        return

    start = (float(trip[START_LATITUDE_IDX]), float(trip[START_LONGITUDE_IDX]))
    end = (float(trip[END_LATITUDE_IDX]), float(trip[END_LONGITUDE_IDX]))
    distance = haversine(start, end)

    filtered = [trip[CITY_IDX], trip[STATION_NAME_IDX], str(distance)]

    rabbit.publish_queue('query3-pipe2', encode(filtered))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(f"action: filter_callback | result: success | msg: published => {filtered} ")


rabbit = RabbitInterface()

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("query3-pipe1", callback)

logging.info(f"action: consuming | result: done")