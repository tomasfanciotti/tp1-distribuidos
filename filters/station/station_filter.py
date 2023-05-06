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

EOF = "#"


def filter_station(ch, method, properties, body):
    """
        input:  [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
        output: [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if reg == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.")
        rabbit.publish_topic("station_topic", encode(reg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    filtered = reg  # No filter applyed

    rabbit.publish_topic("station_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


rabbit = RabbitInterface()

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_station_data", filter_station)

logging.info(f"action: consuming | result: done ")
