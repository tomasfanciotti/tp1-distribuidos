# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController           # module provided on the container
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "q2-filter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: filter | result: startup | node_id: {NODE_ID}")


YEARS = [2016, 2017]

CITY_IDX = 0
STATION_IDX = 2
YEAR_IDX = 5
STATION_NAME_IDX = 6


def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def callback(ch, method, properties, body):
    """
        input: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE ]
        output: [CITY, START_STATION, YEAR]
    """

    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip}")

    if int(trip[YEAR_IDX]) not in YEARS:
        logging.debug(f"action: filter_callback | result: success | msg: filtered trip by YEAR")
        return

    filtered = [trip[CITY_IDX], trip[STATION_NAME_IDX], trip[YEAR_IDX]]
    rabbit.publish_queue("query2-pipe1", encode(filtered))
    logging.debug(f"action: filter_callback | result: success | msg: published trip filtered {filtered}")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)
rabbit.bind_topic("trip-start-station-topic", "")

logging.info(f"action: consuming trip-start-stations | result: in_progress ")
rabbit.consume_topic(callback)

logging.info(f"action: consuming trip-start-stations| result: done ")
rabbit.disconnected()
