import logging
import os
# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
# noinspection PyUnresolvedReferences
from batching import Batching

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

CONFIG_STAGE = "joiner_query2_config"
JOIN_STAGE = "joiner_query2_join"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: trip_station_joiner | result: startup | node_id: {NODE_ID}")

# station file
CITY_ID = 0
STATION_ID = 1
STATION_NAME = 2
STATION_YEAR_ID = 5


# Trip file
TRIP_CITY_ID = 0
TRIP_START_STATION = 1
TRIP_YEAR = 2

station_info = {}

Q2_YEARS_FILTER = ['2016', '2017']


def log_eof(ch, method, properties, body):
    batching.push_buffer()
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def config_station(ch, method, properties, body):
    """
        input:  [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
        output: None
    """

    station = decode(body)
    logging.debug(f"action: config_callback | result: in_progress | body: {station} ")

    key = (station[CITY_ID], station[STATION_ID], station[STATION_YEAR_ID])
    value = station
    # [station[i] for i in range(len(station)) if i != STATION_ID and i != STATION_YEAR_ID and i != CITY_ID]

    if key in station_info:
        logging.warning(f"action: config_callback | result: warning | msg: key {key} already station info. Overwriting")

    station_info[key] = value
    logging.debug(f"action: config_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):

    """
        input:  [ CITY, START_STATION, YEAR ]
        output: [ CITY, START_STATION_NAME, YEAR ]
    """

    trip = decode(body)
    logging.info(f"action: join_callback | result: in_progress | msg: {trip} ")

    trip_start_station = (trip[TRIP_CITY_ID], trip[TRIP_START_STATION], trip[TRIP_YEAR])

    if trip_start_station not in station_info:
        logging.debug(
            f"action: join_callback | result: warning | msg: No station info found for trip STARTED in {trip_start_station}. Ignoring join..")
        return

    station = station_info[trip_start_station]
    joined = [trip[CITY_ID], station[STATION_NAME], trip[TRIP_YEAR]]

    batching.publish_batch_to_queue("collector_q2", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: trip and START station joined -> {joined} ")


rabbit = EOFController(CONFIG_STAGE, NODE_ID, on_eof=log_eof, stop_on_eof=True)
rabbit.bind_topic("station_topic", "", dest="stations")

batching = Batching(rabbit)

logging.info(f"action: consuming stations | result: in_progress ")
batching.consume_batch_topic(config_station, dest="stations")

rabbit.set_stage(JOIN_STAGE)
rabbit.set_on_eof(log_eof, stop=False)

logging.info(f"action: consuming trips | result: in_progress ")
batching.consume_batch_queue("trip_to_joiner_q2", joiner)

logging.info(f"action: consuming trips | result: done ")

rabbit.disconnect()
