# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode
import pika
import time
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Wait for rabbitmq to come up
time.sleep(10)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# Publish filtered
channel.queue_declare(queue='query2-pipe1', durable=True)
channel.queue_declare(queue='query2-pipe2', durable=True)

STATION_NAME_IDX = 0
YEAR_IDX = 1

EOF = "#"

status = {}


def callback(ch, method, properties, body):
    trip = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        return

    station = trip[STATION_NAME_IDX]
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
    logging.info(
        f"action: filter_callback | result: success | key: {station}, {year} | counter : {status[station][year]} ")


def filter_results(final_status):
    for station in final_status:
        trips_2016 = final_status[station]['2016']
        trips_2017 = final_status[station]['2017']

        if 2 * trips_2016 < trips_2017:
            channel.basic_publish(exchange="",
                                  routing_key="query2-pipe2",
                                  body=encode(station))
            logging.info(
                f"action: response_enqueue | result: success | selected: {station} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")

        else:
            logging.info(
                f"action: response_enqueue | result: success | discarded: {station} "
                f"| 2016: {trips_2016} | 2017: {trips_2017} ")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='query2-pipe1', on_message_callback=callback, auto_ack=False)

logging.info(
    f"action: counter | result: in_progress | msg: start consuming from query2-pipe1 ")

channel.start_consuming()

filter_results(status)

logging.info(
    f"action: counter | result: success ")
