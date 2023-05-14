# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
import logging,os

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

STAGE = "raw_station_filter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: station_fiter | result: startup | node_id: {NODE_ID}")


def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF - {body}")


def filter_station(ch, method, properties, body):
    """
        input:  [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
        output: [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    filtered = reg  # No filter applyed

    rabbit.publish_topic("station_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)   # Instancia de RabbitInterface con un controller para el EOF

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_station_data", filter_station)

logging.info(f"action: consuming | result: done ")
rabbit.disconnect()