# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface
# noinspection PyUnresolvedReferences
from eof import EOF
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "raw_weather_filter"
NODE_NAME = "1"

CITY_INDEX = 0
DATE_INDEX = 1
PRECTOT_INDEX = 3


def filter_weather(ch, method, properties, body):
    """
        input:  [ CITY, DATE, PRECTOT, QV2M, RH2M, PS, T2M_RANGE, TS, ... ]
        output: [ CITY, DATE, PRECTOT ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if EOF.is_eof(reg):
        if properties.headers is None:
            logging.error(f"action: eof | result: done | msg: no encabzad detectado.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        original = properties.headers.get('original')
        rabbit.publish_queue("EOF_queue", EOF(STAGE, NODE_NAME).encode(), headers={"original": original})
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"action: eof | result: success | msg: END OF FILE trips | original: {original}")
        return

    filtered = [reg[CITY_INDEX], reg[DATE_INDEX], reg[PRECTOT_INDEX]]

    rabbit.publish_topic("weather_topic", encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


rabbit = RabbitInterface()

logging.info(f"action: register_manager | result: in_progress ")
rabbit.publish_queue("EOF_queue", EOF.create_register(STAGE, NODE_NAME).encode())

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_weather_data", filter_weather)

logging.info(f"action: consuming | result: done ")
