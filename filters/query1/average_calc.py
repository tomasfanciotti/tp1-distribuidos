# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from eof_controller import EOFController
# noinspection PyUnresolvedReferences
from result import Result
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

STAGE = "average_calc"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: average_filter | result: startup | node_id: {NODE_ID}")

DURATION_INDEX = 1

status = {"trips": 0,
          "duration_sum": 0}


def avg(ch, method, properties, body):

    batching.push_buffer()

    logging.info(f"action: callback | result: success | msg: received EOF of trips - {body}")

    if status["trips"] == 0:
        logging.info(
            f"action: response | result: fail | trips: {status['trips']} | duration_sum : {status['duration_sum']} "
            f"| msg: no trips loaded")

    else:

        data = str(round(status["duration_sum"] / status["trips"], 4))
        result = Result.query1(data)
        rabbit.publish_queue("query_results", result.encode())
        logging.info(f"action: response_enqueue | result: success | {result} | trips: {status['trips']}")

        # Reset Status
        status["trips"] = 0
        status["duration_sum"] = 0


def callback(ch, method, properties, body):
    """
        input:  [ CITY, DURATION ]
        output: [ AVERAGE ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    status["trips"] += 1
    status["duration_sum"] += float(trip[DURATION_INDEX])

    logging.debug(
        f"action: filter_callback | result: success | trips: {status['trips']} | duration_sum : {status['duration_sum']}")


rabbit = EOFController(STAGE, NODE_ID, on_eof=avg)

logging.info(f"action: consuming trip-weathers | result: in_progress ")

batching = Batching(rabbit)
batching.consume_batch_queue("collector_q1", callback)

logging.info(f"action: consuming trip-weathers | result: done")

rabbit.disconnect()
