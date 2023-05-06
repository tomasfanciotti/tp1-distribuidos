# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

DURATION_IDX = 4
PRECTOT_IDX = 6

PRECTOT_TRESHOLD = 1
EOF = "#"


def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, PRECTOT ]
        output: [ DURATION ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE trips.")
        rabbit.publish_queue('query1-pipe1', encode(trip))
        return

    if float(trip[PRECTOT_IDX]) > PRECTOT_TRESHOLD:
        rabbit.publish_queue('query1-pipe1', encode([trip[DURATION_IDX]]))
        logging.debug(f"action: filter_callback | result: success | msg: condition met. Sending to the next stage.")
    else:
        logging.debug(f"action: filter_callback | result: success | msg: trip filtered.")


rabbit = RabbitInterface()
rabbit.bind_topic("trip-weather-topic", "")

logging.info(f"action: consuming trip-weathers | result: in_progress ")
rabbit.consume_topic(callback)

logging.info(f"action: consuming trip-weathers | result: done ")
