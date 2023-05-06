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

# station file
CITY_ID = 0
STATION_ID = 1
STATION_YEAR_ID = 5

# Trip file
TRIP_CITY_ID = 0
START_STATION = 2
END_STATION = 3
TRIP_YEAR_ID = 5

station_info = {}

EOF = "#"


def config_station(ch, method, properties, body):
    """
        input:  [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
        output: None
    """

    station = decode(body)
    logging.debug(f"action: config_callback | result: in_progress | body: {station} ")

    if station == EOF:
        logging.info(f"action: config_callback | result: success | msg: END OF FILE station.")
        ch.stop_consuming()
        return

    key = (station[CITY_ID], station[STATION_ID], station[STATION_YEAR_ID])
    value = [station[i] for i in range(len(station)) if i != STATION_ID and i != STATION_YEAR_ID and i != CITY_ID]

    if key in station_info:
        logging.warning(f"action: config_callback | result: warning | msg: key {key} already station info. Overwriting")

    station_info[key] = value
    logging.debug(f"action: config_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):

    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
        output1: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE ]
        output2: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, END_DATENAME, END_LATITUDE, END_LONGITUDE ]
    """

    trip = decode(body)
    logging.info(f"action: join_callback | result: in_progress | msg: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE trips.") 
        rabbit.publish_topic("trip-start-station-topic", encode(trip))
        rabbit.publish_topic("trip-end-station-topic", encode(trip))
        return

    trip_start_station = (trip[CITY_ID], trip[START_STATION], trip[TRIP_YEAR_ID])
    trip_end_station = (trip[CITY_ID], trip[END_STATION], trip[TRIP_YEAR_ID])

    if trip_start_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip STARTED in {trip_start_station}. Ignoring join..")
        return

    if trip_end_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip ENDED in {trip_end_station}. Ignoring join..")
        return

    station = station_info[trip_start_station]
    joined = trip + station
    logging.debug(f"action: join_callback | result: in_progress | msg: trip and START station joined -> {joined} ")

    rabbit.publish_topic("trip-start-station-topic", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: published join in topic")

    station = station_info[trip_end_station]
    joined = trip + station
    logging.debug(f"action: join_callback | result: in_progress | msg: trip and END station joined -> {joined} ")

    rabbit.publish_topic("trip-end-station-topic", encode(joined))
    logging.debug(f"action: join_callback | result: success | msg: published join in topic")


rabbit = RabbitInterface()
rabbit.bind_topic("station_topic", "", dest="stations")
rabbit.bind_topic("trip_topic", "", dest="trips")

logging.info(f"action: consuming stations | result: in_progress ")
rabbit.consume_topic(config_station, dest="stations" )

logging.info(f"action: consuming trips | result: in_progress ")
rabbit.consume_topic(joiner, dest="trips" )

logging.info(f"action: consuming trips | result: done ")
