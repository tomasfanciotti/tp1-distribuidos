# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 3

EOF = "#"


def filter_weather(ch, method, properties, body):
    """
        input:  [ CITY, DATE, PRECTOT, QV2M, RH2M, PS, T2M_RANGE, TS, ... ]
        output: [ CITY, DATE, PRECTOT ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if reg == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.")
        rabbit.publish_topic("weather_topic", encode(reg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    filtered = [reg[CITY_INDEX], reg[DATE_INDEX], reg[PRECTOT_INDEX]]

    rabbit.publish_topic("weather_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


rabbit = RabbitInterface()

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_weather_data", filter_weather)

logging.info(f"action: consuming | result: done ")
