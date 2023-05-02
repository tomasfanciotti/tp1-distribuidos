# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container
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
channel.queue_declare(queue='query3-pipe2', durable=True)
channel.queue_declare(queue='query3-pipe3', durable=True)


STATION_IDX = 0
DISTANCE_IDX = 1

EOF = "#"

status = {}


def callback(ch, method, properties, body):
    """
        input:  [END_STATION, DISTANCE]
        output: []
    """
    
    trip = decode(body)
    logging.info(f"action: callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"action: callback | result: done | Received EOF ")
        ch.basic_publish(exchange="", routing_key='query3-pipe3', body=encode(EOF))
        ch.stop_consuming()
        return

    if trip[STATION_IDX] not in status:
        logging.info(f"action: callback | result: in_progress | msg: new station.")
        status[trip[STATION_IDX]] = { "trips": 0,
                                      "distance_sum": 0}

    status[trip[STATION_IDX]]["trips"] += 1
    status[trip[STATION_IDX]]["distance_sum"] += float(trip[DISTANCE_IDX])

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(
        f"action: callback | result: success | trips: {status[trip[STATION_IDX]]['trips']} | distance_sum : { status[trip[STATION_IDX]]['distance_sum']}.")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='query3-pipe2', on_message_callback=callback, auto_ack=False)

logging.info(
    f"action: average_calc | result: in_progress | msg: start consuming from queue: query3-pipe2")

channel.start_consuming()

logging.info(
    f"action: average_calc | result: in_progress | msg: calculating averages")

for station in status:

    result = str(round(status[station]["distance_sum"] / status[station]["trips"], 4))
    response = [station, result]

    channel.basic_publish(exchange="",
                          routing_key="query3-pipe3",
                          body=encode(response))
    logging.info(
        f"action: average_calc | result: in_progress | station: {station} | average: {result}")


logging.info(
    f"action: average_calc | result: done ")
