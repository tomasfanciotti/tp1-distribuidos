# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface  # module provided on the container
# noinspection PyUnresolvedReferences
from eof import EOF, send_EOF, add_listener  # module provided on the container
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


def callback(ch, method, properties, body):
    """
        input:  [ DURATION ]
        output: [ AVERAGE ]
    """
    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if EOF.is_eof(trip):
        result, msg = send_EOF(STAGE, NODE_ID, properties, rabbit)
        logging.info(f"action: eof | result: {result} | msg: {msg}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        return

    status["trips"] += 1
    status["duration_sum"] += float(trip[DURATION_INDEX])

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(
        f"action: filter_callback | result: success | trips: {status['trips']} | duration_sum : {status['duration_sum']} ")


rabbit = RabbitInterface()
add_listener(STAGE, NODE_ID, rabbit)

logging.info(f"action: consuming trip-weathers | result: in_progress ")
rabbit.consume_queue("query1-pipe1", callback)

logging.info(f"action: consuming trip-weathers | result: done")

if status["trips"] == 0:
    logging.info(
        f"action: response | result: fail | trips: {status['trips']} | duration_sum : {status['duration_sum']} | msg: no trips loaded")

else:

    result = str(round(status["duration_sum"] / status["trips"], 4))

    rabbit.publish_queue("query1-pipe2", encode(result))

    logging.info(f"action: response_enqueue | result: success | result: {result} | trips: {status['trips']}")
