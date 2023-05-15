# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController           # module provided on the container
# noinspection PyUnresolvedReferences
from batching import Batching
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "q3-join"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: join_stations | result: startup | node_id: {NODE_ID}")


SOURCES = ['trip-start-station-topic', 'trip-end-station-topic']

TARGET = "montreal"

ID_TRIP_IDX = 0
CITY_IDX = 0
STATION_IDX = 6
LATITUDE_IDX = 7
LONGITUDE_IDX = 8

status = {}
topic_EOF = set()


def log_eof(ch, method, properties, body):
    batching.push_buffer()
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE]
        input2: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, END_NAME, END_LATITUDE, END_LONGITUDE]
        output: [ CITY, END_NAME, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
    """

    trip = decode(body)
    logging.debug(f"action: callback | result: in_progress | from: {method.exchange} | body: {trip} ")

    if trip[CITY_IDX] != TARGET:
        logging.warning(f"action: callback | result: done | msg: trip from other city instead of {TARGET}")
        return

    trip_id = tuple(trip[:6])
    source = method.exchange
    if trip_id not in status:
        status[trip_id] = {}
        logging.info(f"action: callback | result: in_progress | msg: new trip in satus.")

    if source not in status[trip_id]:
        status[trip_id][source] = trip

    if len(status[trip_id].keys()) == 2:
        logging.debug(f"action: callback | result: in_progress | msg: ready to join")

        start_data = status[trip_id]["trip-start-station-topic"]
        end_data = status[trip_id]["trip-end-station-topic"]
        output = [ end_data[CITY_IDX], end_data[STATION_IDX],
                  start_data[LATITUDE_IDX], start_data[LONGITUDE_IDX],
                  end_data[LATITUDE_IDX], end_data[LONGITUDE_IDX]]

        batching.publish_batch_to_queue('query3-pipe1', encode(output))
        del status[trip_id]
        logging.debug(f"action: callback | result: success | msg: joined and published => {output}")

    else:
        logging.debug(f"action: callback | result: success | msg: sotored en status and waiting for next station")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)
rabbit.bind_topic("trip-start-station-topic", "")
rabbit.bind_topic("trip-end-station-topic", "")

batching = Batching(rabbit)

logging.info(f"action: consuming trips | result: in_progress ")
batching.consume_batch_topic(callback)

logging.info(f"action: join_station | result: success ")
