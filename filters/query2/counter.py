# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
# noinspection PyUnresolvedReferences
from eof_controller import EOFController           # module provided on the container
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

STAGE = "q2-counter"
NODE_ID = os.environ.get('HOSTNAME')
logging.info(f"action: counter | result: startup | node_id: {NODE_ID}")

CITY_IDX = 0
STATION_NAME_IDX = 1
YEAR_IDX = 2

status = {}


def callback(ch, method, properties, body):
    """
        input: [ CITY, START_STATION_NAME, YEAR ]
        output: [CITY, STATION]
    """

    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    station = (trip[CITY_IDX], trip[STATION_NAME_IDX])
    year = trip[YEAR_IDX]

    if station not in status:
        logging.info(f"action: filter_callback | result: in_progress | msg: new station-year key stored ")
        status[station] = {'2016': 0, '2017': 0}

    if year not in status[station]:
        logging.warning(f"action: filter_callback | result: warning | msg: invalid YEAR ID. Ignoring.. ")
        return

    status[station][year] += 1

    logging.debug(
        f"action: filter_callback | result: success | key: {station}, {year} | counter : {status[station][year]} ")


def filter_results(ch, method, properties, body):

    logging.info(f"action: callback | result: success | msg: received EOF - {body}")

    copy = status.copy()
    for station in copy:
        trips_2016 = status[station]['2016']
        trips_2017 = status[station]['2017']

        if 2 * trips_2016 < trips_2017:
            data = [station[0], station[1], str(trips_2016), str(trips_2017)]
            result = Result.query2(encode(data))
            rabbit.publish_queue("query_results", result.encode())
            logging.info(
                f"action: response_enqueue | result: success | city SELECETED: {station[0]} | station: {station[1]} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")

        else:
            logging.debug(
                f"action: response_enqueue | result: success | city DISCARDED: {station[0]} | station: {station[1]} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")

        del status[station]


rabbit = EOFController(STAGE, NODE_ID, on_eof=filter_results)
batching = Batching(rabbit)

logging.info(f"action: consuming | result: in_progress ")
batching.consume_batch_queue("collector_q2", callback)

logging.info(f"action: consuming | result: done")

logging.info(f"action: counter | result: success ")

rabbit.disconnect()
