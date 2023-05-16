# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
# noinspection PyUnresolvedReferences
from batching import Batching
import logging
import os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "raw_weather_filter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: weather_filter | result: startup | node_id: {NODE_ID}")

CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 2


def log_eof(ch, method, properties, body):
    batching.push_buffer()
    logging.info(f"action: callback | result: success | msg: received EOF -> {body}")


def filter_weather(ch, method, properties, body):
    """
        input:  [ CITY, DATE, PRECTOT, QV2M, RH2M, PS, T2M_RANGE, TS, ... ]
        output: [ CITY, DATE, PRECTOT ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    filtered = [reg[CITY_INDEX], reg[DATE_INDEX], reg[PRECTOT_INDEX]]

    batching.publish_batch_to_topic("weather_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    logging.info(f"action: filter_callback | result: success ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)   # Instancia de RabbitInterface con un controller para el EOF
batching = Batching(rabbit)

logging.info(f"action: consuming | result: in_progress ")
batching.consume_batch_queue("raw_weather_data", filter_weather)

logging.info(f"action: consuming | result: done ")
rabbit.disconnect()
