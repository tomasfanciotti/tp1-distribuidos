# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
# noinspection PyUnresolvedReferences
from rabbit_interface import RabbitInterface    # module provided on the container
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

CITY_IDX = 0
STATION_NAME_IDX = 1
YEAR_IDX = 2

EOF = "#"

status = {}


def callback(ch, method, properties, body):
    """
        input: [CITY, START_STATION, YEAR]
        output: [CITY, STATION]
    """

    trip = decode(body)
    logging.debug(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        return

    station = (trip[CITY_IDX], trip[STATION_NAME_IDX])
    year = trip[YEAR_IDX]

    if station not in status:
        logging.info(f"action: filter_callback | result: in_progress | msg: new station-year key stored ")
        status[station] = {'2016': 0, '2017': 0}

    if year not in status[station]:
        logging.warning(f"action: filter_callback | result: warning | msg: invalid YEAR ID. Ignoring.. ")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    status[station][year] += 1

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.debug(
        f"action: filter_callback | result: success | key: {station}, {year} | counter : {status[station][year]} ")


def filter_results(final_status):
    for station in final_status:
        trips_2016 = final_status[station]['2016']
        trips_2017 = final_status[station]['2017']

        if 2 * trips_2016 < trips_2017:
            result = [station[0], station[1], str(trips_2016), str(trips_2017)]
            rabbit.publish_queue("query2-pipe2", encode(result))
            logging.info(
                f"action: response_enqueue | result: success | city SELECETED: {station[0]} | station: {station[1]} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")

        else:
            logging.info(
                f"action: response_enqueue | result: success | city DISCARDED: {station[0]} | station: {station[1]} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")


rabbit = RabbitInterface()

logging.info(f"action: consuming | result: in_progress ")
rabbit.consume_queue("query2-pipe1", callback)

logging.info(f"action: consuming | result: done")

filter_results(status)

logging.info(f"action: counter | result: success ")
