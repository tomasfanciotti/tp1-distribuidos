# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode  # module provided on the container
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

channel.exchange_declare(exchange='trip-start-station-topic', exchange_type='fanout')
channel.exchange_declare(exchange='trip-end-station-topic', exchange_type='fanout')

# Collect joined trips
result = channel.queue_declare(queue='', durable=True)
queue_name = result.method.queue
channel.queue_bind(exchange='trip-start-station-topic', queue=queue_name)
channel.queue_bind(exchange='trip-end-station-topic', queue=queue_name)

# Publish filtered
channel.queue_declare(queue='query3-pipe1', durable=True)

SOURCES = ['trip-start-station-topic', 'trip-end-station-topic']

TARGET = "montreal"

ID_TRIP_IDX = 0
CITY_IDX = 0
STATION_IDX = 6
LATITUDE_IDX = 7
LONGITUDE_IDX = 8

EOF = "#"

status = {}
topic_EOF = set()


def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE]
        input2: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, END_NAME, END_LATITUDE, END_LONGITUDE]
        output: [ CITY, END_NAME, START_LATITUDE, START_LONGITUDE, END_LATITUDE, END_LONGITUDE]
    """

    trip = decode(body)
    logging.info(f"action: callback | result: in_progress | from: {method.exchange} | body: {trip} ")

    if trip == EOF:
        topic_EOF.add(method.exchange)
        if len(topic_EOF) >= len(SOURCES):
            logging.info(f"action: callback | result: done | msg: END OF FILE trips.")
            ch.basic_publish(exchange="", routing_key='query3-pipe1', body=encode(trip))
        return

    if trip[CITY_IDX] != TARGET:
        logging.info(f"action: callback | result: done | msg: trip from other city instead of {TARGET}")
        return

    trip_id = tuple(trip[:6])
    source = method.exchange
    if trip_id not in status:
        status[trip_id] = {}
        logging.info(f"action: callback | result: in_progress | msg: new trip in satus.")

    if source not in status[trip_id]:
        status[trip_id][source] = trip

    if len(status[trip_id].keys()) == 2:
        logging.info(f"action: callback | result: in_progress | msg: ready to join")

        start_data = status[trip_id]["trip-start-station-topic"]
        end_data = status[trip_id]["trip-end-station-topic"]
        output = [ end_data[CITY_IDX], end_data[STATION_IDX],
                  start_data[LATITUDE_IDX], start_data[LONGITUDE_IDX],
                  end_data[LATITUDE_IDX], end_data[LONGITUDE_IDX]]

        ch.basic_publish(exchange="", routing_key='query3-pipe1', body=encode(output))
        del status[trip_id]
        logging.info(f"action: callback | result: success | msg: joined and published => {output}")

    else:
        logging.info(f"action: callback | result: success | msg: sotored en status and waiting for next station")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

logging.info(
    f"action: join_station | result: in_progress | msg: start consuming from trip-start-station-topic and trip-start-station-topic ")

channel.start_consuming()

logging.info(
    f"action: join_station | result: success ")
