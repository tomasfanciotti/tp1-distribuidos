# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface  # module provided on the container
# noinspection PyUnresolvedReferences
from eof import EOF, send_EOF  # modules provided on the container
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

STAGE = "raw_trip_filter"
NODE_ID = "1"

CITY_INDEX = 0
START_DATE_INDEX = 1
START_STATION_INDEX = 2
END_DATE_INDEX = 3
END_STATION_INDEX = 4
DURATION_INDEX = 5
MEMBER_INDEX = 6
YEAR_INDEX = 7


def filter_trip(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_DATE, END_STATION, DURATION, MEMBER, YEAR]
        output: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
    """

    reg = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {reg} ")

    if EOF.is_eof(reg):
        result, msg = send_EOF(STAGE, NODE_ID, properties, rabbit)
        logging.info(f"action: eof | result: {result} | msg: {msg}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if reg[DURATION_INDEX][1:].replace(".", "").isdigit():

        if float(reg[DURATION_INDEX]) < 0:
            reg[DURATION_INDEX] = "0"

        filtered = [reg[CITY_INDEX], reg[START_DATE_INDEX], reg[START_STATION_INDEX], reg[END_STATION_INDEX],
                    reg[DURATION_INDEX],
                    reg[YEAR_INDEX]]

        rabbit.publish_topic("trip_topic", encode(filtered))

        logging.debug(f"action: filter_callback | result: in_progress | filtered: {filtered} ")
    else:
        logging.error(f"action: filter_callback | result: fail | ignored due type error | data: {reg} ")

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(f"action: filter_callback | result: success.")


rabbit = RabbitInterface()

logging.info(f"action: register_manager | result: in_progress ")
rabbit.publish_queue("EOF_queue", EOF.create_register(STAGE, NODE_ID).encode())

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_trip_data", filter_trip)

logging.info(f"action: consuming | result: done ")
