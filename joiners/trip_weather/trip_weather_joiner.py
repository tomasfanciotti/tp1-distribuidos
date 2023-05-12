
import logging

# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
# noinspection PyUnresolvedReferences
from eof import EOF, send_EOF, add_listener  # module provided on the container

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

CONFIG_STAGE = "weather_joiner_config"
JOIN_STAGE = "weather_join"
NODE_ID = "1"


weather_info = {}

# Weather file
CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 2

# Trip file
TRIP_CITY_INDEX = 0
START_DATE_INDEX = 1


def config_weather(ch, method, properties, body):
    """
    input:  [ CITY, DATE, PRECTOT ]
    output: None
    """

    weather = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | msg: {weather} ")

    if EOF.is_eof(weather):
        result, msg = send_EOF(CONFIG_STAGE, NODE_ID, properties, rabbit)
        logging.info(f"action: eof | result: {result} | msg: {msg}")
        ch.stop_consuming()
        return

    key = (weather[CITY_INDEX], weather[DATE_INDEX].split(" ")[0])
    value = [weather[PRECTOT_INDEX]]

    if key in weather_info:
        logging.warning(f"action: filter_callback | result: warning | msg: key {key} already weather info. Overwriting")

    weather_info[key] = value
    logging.debug(f"action: filter_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):
    """
    input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
    output: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, PRECTOT]
    """

    trip = decode(body)
    logging.debug(f"action: join_callback | result: in_progress | msg: {trip} ")

    if EOF.is_eof(trip):
        result, msg = send_EOF(JOIN_STAGE, NODE_ID, properties, rabbit)
        logging.info(f"action: eof | result: {result} | msg: {msg}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    trip_weather = ( trip[TRIP_CITY_INDEX], trip[START_DATE_INDEX].split(" ")[0])
    if trip_weather not in weather_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No weather info found for trip started in {trip_weather}. Ignoring join..")
        return

    wheather = weather_info[trip_weather]
    joined = trip + wheather
    logging.debug(f"action: join_callback | result: in_progress | msg: trip and weather joined -> {joined} ")

    rabbit.publish_topic("trip-weather-topic", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: published join in topic")


rabbit = RabbitInterface()
rabbit.bind_topic("weather_topic", "", dest="weathers")
rabbit.bind_topic("trip_topic", "", dest="trips")

logging.info(f"action: register_manager | result: in_progress ")
add_listener(CONFIG_STAGE, NODE_ID, rabbit)
add_listener(JOIN_STAGE, NODE_ID, rabbit)

# Weathers
logging.info(f"action: consuming weathers | result: in_progress ")
rabbit.consume_topic(config_weather, dest="weathers")

# Trips
logging.info(f"action: consuming trips | result: in_progress ")
rabbit.consume_topic(joiner, dest="trips")

logging.info(f"action: consuming trips | result: done ")
