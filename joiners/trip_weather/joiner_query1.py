import logging
import os
# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
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

CONFIG_STAGE = "joiner_query1_config"
JOIN_STAGE = "joiner_query1_join"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: trip_weather_joiner | result: startup | node_id: {NODE_ID}")

weather_info = {}

# Weather file
CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 2

# Trip file
TRIP_CITY_INDEX = 0
START_DATE_INDEX = 1
DURATION= 2


def log_eof(ch, method, properties, body):
    batching.push_buffer()
    logging.info(f"action: callback | result: success | msg: received EOF of trips - {body}")


def config_weather(ch, method, properties, body):
    """
    input:  [ CITY, DATE ]
    output: None
    """

    weather = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | msg: {weather} ")

    key = (weather[CITY_INDEX], weather[DATE_INDEX].split(" ")[0])
    value = [":v"]

    if key in weather_info:
        logging.warning(f"action: filter_callback | result: warning | msg: key {key} already weather info. Overwriting")

    weather_info[key] = value
    logging.debug(f"action: filter_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):
    """
    input:  [ CITY, START_DATE, DURATION]
    output: [ CITY, DURATION]
    """

    trip = decode(body)
    logging.debug(f"action: join_callback | result: in_progress | msg: {trip} ")

    trip_weather = (trip[TRIP_CITY_INDEX], trip[START_DATE_INDEX].split(" ")[0])
    if trip_weather not in weather_info:
        logging.debug(
            f"action: join_callback | result: warning | msg: No weather info found for trip started in {trip_weather}. Ignoring join..")
        return

    wheather = weather_info[trip_weather],

    joined = [trip[CITY_INDEX], trip[DURATION]]
    logging.debug(f"action: join_callback | result: in_progress | msg: trip and weather joined -> {joined} ")

    batching.publish_batch_to_queue("collector_q1", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: published join in topic")


rabbit = EOFController(CONFIG_STAGE, NODE_ID, on_eof=log_eof, stop_on_eof=True)
rabbit.bind_topic("weather_topic", "", dest="weathers")

batching = Batching(rabbit)

# Weathers
logging.info(f"action: consuming weathers | result: in_progress ")
batching.consume_batch_topic(config_weather, dest="weathers")

rabbit.set_stage(JOIN_STAGE)
rabbit.set_on_eof(log_eof, stop=False)

# Trips
logging.info(f"action: consuming trips | result: in_progress ")
batching.consume_batch_queue("trip_to_joiner_q1", joiner)

rabbit.disconnect()
logging.info(f"action: consuming trips | result: done ")
