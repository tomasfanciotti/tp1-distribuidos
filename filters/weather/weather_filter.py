# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
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
print("Hostname: ", NODE_ID)

CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 2


def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF")


def filter_weather(ch, method, properties, body):
    """
        input:  [ CITY, DATE, PRECTOT, QV2M, RH2M, PS, T2M_RANGE, TS, ... ]
        output: [ CITY, DATE, PRECTOT ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    filtered = [reg[CITY_INDEX], reg[DATE_INDEX], reg[PRECTOT_INDEX]]

    rabbit.publish_topic("weather_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)   # Instancia de RabbitInterface con un controller para el EOF

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_weather_data", filter_weather)

logging.info(f"action: consuming | result: done ")
rabbit.disconnect()
