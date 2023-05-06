# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

CITY_IDX = 0
STATION_IDX = 1
DISTANCE_IDX = 2

EOF = "#"

status = {}


def callback(ch, method, properties, body):
    """
        input:  [CITY, END_STATION, DISTANCE]
        output: []
    """
    
    trip = decode(body)
    logging.debug(f"action: callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"action: callback | result: done | Received EOF ")
        rabbit.publish_queue('query3-pipe3', encode(EOF))
        ch.stop_consuming()
        return

    key = (trip[CITY_IDX], trip[STATION_IDX])
    if key not in status:
        logging.info(f"action: callback | result: in_progress | msg: new station.")
        status[key] = {"trips": 0, "distance_sum": 0}

    status[key]["trips"] += 1
    status[key]["distance_sum"] += float(trip[DISTANCE_IDX])

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(
        f"action: callback | result: success | trips: {status[key]['trips']} | distance_sum : { status[key]['distance_sum']}.")


rabbit = RabbitInterface()

logging.info(f"action: average_calc | result: in_progress | msg: start consuming from queue: query3-pipe2")

rabbit.consume_queue("query3-pipe2", callback)

logging.info(f"action: average_calc | result: in_progress | msg: calculating averages")

for station in status:

    result = str(round(status[station]["distance_sum"] / status[station]["trips"], 4))
    response = [station[0], station[1], result]

    rabbit.publish_queue("query3-pipe3", encode(response))
    logging.info(
        f"action: average_calc | result: in_progress | city: {station[0]} | station: {station[1]} | average: {result}")

logging.info(
    f"action: average_calc | result: done ")
