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

CONFIG_STAGE = "joiner_query3_config"
JOIN_STAGE = "joiner_query3_join"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: trip_station_joiner | result: startup | node_id: {NODE_ID}")

# station file
CITY_ID = 0
STATION_ID = 1
STATION_NAME = 2
STATION_LATITUDE = 3
STATION_LONGITUDE = 4
STATION_YEAR_ID = 5

# Trip file
TRIP_CITY_ID = 0
START_STATION = 1
END_STATION = 2
TRIP_YEAR_ID = 3

station_info = {}


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
        input:  [ CITY, START_STATION, END_STATION, YEAR ]
        output: [ CITY, END_NAME, YEAR, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
    """

    trip = decode(body)
    logging.info(f"action: join_callback | result: in_progress | msg: {trip} ")

    trip_start_station = (trip[TRIP_CITY_ID], trip[START_STATION], trip[TRIP_YEAR_ID])
    trip_end_station = (trip[TRIP_CITY_ID], trip[END_STATION], trip[TRIP_YEAR_ID])

    if trip_start_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip STARTED in {trip_start_station}. Ignoring join..")
        return

    if trip_end_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip ENDED in {trip_end_station}. Ignoring join..")
        return

    start_station_info = station_info[trip_start_station]
    end_station_info = station_info[trip_end_station]
    joined = [end_station_info[CITY_ID], end_station_info[STATION_NAME], end_station_info[STATION_YEAR_ID],
              start_station_info[STATION_LATITUDE], start_station_info[STATION_LONGITUDE],
              end_station_info[STATION_LATITUDE], end_station_info[STATION_LONGITUDE]]

    batching.publish_batch_to_queue("collector_q3", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: trip and START and END station joined -> {joined} ")


rabbit = EOFController(CONFIG_STAGE, NODE_ID, on_eof=log_eof, stop_on_eof=True)
rabbit.bind_topic("station_topic", "", dest="stations")

batching = Batching(rabbit)

logging.info(f"action: consuming stations | result: in_progress ")
batching.consume_batch_topic(config_station, dest="stations")

rabbit.set_stage(JOIN_STAGE)
rabbit.set_on_eof(log_eof, stop=False)

logging.info(f"action: consuming trips | result: in_progress ")
batching.consume_batch_queue("trip_to_joiner_q3", joiner)

logging.info(f"action: consuming trips | result: done ")

rabbit.disconnect()
