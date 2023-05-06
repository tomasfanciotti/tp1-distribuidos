
import logging

# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

weather_info = {}

# Weather file
CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 2

# Trip file
TRIP_CITY_INDEX = 0
START_DATE_INDEX = 1

EOF = "#"


def config_weather(ch, method, properties, body):
    """
    input:  [ CITY, DATE, PRECTOT ]
    output: None
    """

    weather = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | msg: {weather} ")

    if weather == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE weather.")
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
    
    if trip == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.") 
        rabbit.publish_topic("trip-weather-topic",  encode(trip))
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

logging.info(f"action: consuming weathers | result: in_progress ")
rabbit.consume_topic(config_weather, dest="weathers")

logging.info(f"action: consuming trips | result: in_progress ")
rabbit.consume_topic(joiner, dest="trips")

logging.info(f"action: consuming trips | result: done ")
