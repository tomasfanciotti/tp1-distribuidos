# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE="prectot_filter"
NODE_ID="1"

DURATION_IDX = 4
PRECTOT_IDX = 6

PRECTOT_TRESHOLD = 1

def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF of trips - {body}")


def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, PRECTOT ]
        output: [ DURATION ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip}")

    if float(trip[PRECTOT_IDX]) > PRECTOT_TRESHOLD:
        rabbit.publish_queue('query1-pipe1', encode([trip[DURATION_IDX]]))
        logging.debug(f"action: filter_callback | result: success | msg: condition met. Sending to the next stage.")
    else:
        logging.debug(f"action: filter_callback | result: success | msg: trip filtered.")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)
rabbit.bind_topic("trip-weather-topic", "")

logging.info(f"action: consuming trip-weathers | result: in_progress ")
rabbit.consume_topic(callback)

logging.info(f"action: consuming trip-weathers | result: done ")
rabbit.disconnect()