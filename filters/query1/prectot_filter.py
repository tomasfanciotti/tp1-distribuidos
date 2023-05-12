# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
# noinspection PyUnresolvedReferences
from eof import EOF, send_EOF, add_listener      # module provided on the container
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


def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, PRECTOT ]
        output: [ DURATION ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if EOF.is_eof(trip):
        result, msg = send_EOF(STAGE, NODE_ID, properties, rabbit)
        logging.info(f"action: eof | result: {result} | msg: {msg}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if float(trip[PRECTOT_IDX]) > PRECTOT_TRESHOLD:
        rabbit.publish_queue('query1-pipe1', encode([trip[DURATION_IDX]]))
        logging.debug(f"action: filter_callback | result: success | msg: condition met. Sending to the next stage.")
    else:
        logging.debug(f"action: filter_callback | result: success | msg: trip filtered.")


rabbit = RabbitInterface()
add_listener(STAGE, NODE_ID, rabbit)

rabbit.bind_topic("trip-weather-topic", "")

logging.info(f"action: consuming trip-weathers | result: in_progress ")
rabbit.consume_topic(callback)

logging.info(f"action: consuming trip-weathers | result: done ")
