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

STAGE = "average_calc"
NODE_ID = "1"

DURATION_INDEX = 0

status = {"trips": 0,
          "duration_sum": 0}


def avg(ch, method, properties, body):

    logging.info(f"action: callback | result: success | msg: received EOF of trips - {body}")

    if status["trips"] == 0:
        logging.info(
            f"action: response | result: fail | trips: {status['trips']} | duration_sum : {status['duration_sum']} "
            f"| msg: no trips loaded")

    else:

        result = str(round(status["duration_sum"] / status["trips"], 4))
        rabbit.publish_queue("query1-pipe2", encode(result))
        logging.info(f"action: response_enqueue | result: success | result: {result} | trips: {status['trips']}")

        # Reset Status
        status["trips"] = 0
        status["duration_sum"] = 0


def callback(ch, method, properties, body):
    """
        input:  [ DURATION ]
        output: [ AVERAGE ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    status["trips"] += 1
    status["duration_sum"] += float(trip[DURATION_INDEX])

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(
        f"action: filter_callback | result: success | trips: {status['trips']} | duration_sum : {status['duration_sum']} ")


rabbit = EOFController(STAGE, NODE_ID, on_eof=avg)

logging.info(f"action: consuming trip-weathers | result: in_progress ")
rabbit.consume_queue("query1-pipe1", callback)

logging.info(f"action: consuming trip-weathers | result: done")

rabbit.disconnect()
