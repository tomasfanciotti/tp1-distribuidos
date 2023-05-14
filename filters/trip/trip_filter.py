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

STAGE = "raw_trip_filter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: trip_filter | result: startup | node_id: {NODE_ID}")

CITY_INDEX = 0
START_DATE_INDEX = 1
START_STATION_INDEX = 2
END_DATE_INDEX = 3
END_STATION_INDEX = 4
DURATION_INDEX = 5
MEMBER_INDEX = 6
YEAR_INDEX = 7


def log_eof(ch, method, properties, body):
    logging.info(f"action: callback | result: success | msg: received EOF of trips - {body}")


def filter_trip(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_DATE, END_STATION, DURATION, MEMBER, YEAR]
        output: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
    """

    reg = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {reg} ")

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


rabbit = EOFController(STAGE, NODE_ID, on_eof=log_eof)

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("raw_trip_data", filter_trip)

logging.info(f"action: consuming | result: done ")

rabbit.disconnect()
