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

channel.exchange_declare(exchange='trip-start-station-topic', exchange_type='fanout')

# Collect joined trips
result = channel.queue_declare(queue='', durable=True)
queue_name = result.method.queue
channel.queue_bind(exchange='trip-start-station-topic', queue=queue_name)

# Publish filtered
channel.queue_declare(queue='query2-pipe1', durable=True)

YEARS = [2016, 2017]

CITY_IDX = 0
STATION_IDX = 2
YEAR_IDX = 5
STATION_NAME_IDX = 6

EOF="#"

def callback(ch, method, properties, body):
    """
        input: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE ]
        output: [CITY, START_STATION, YEAR]
    """

    trip = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.") 
        channel.basic_publish(exchange="",  routing_key='query2-pipe1', body=encode(trip))
        return

    if int(trip[YEAR_IDX]) not in YEARS:
        logging.info(f"action: filter_callback | result: success | msg: filtered trip by YEAR")
        return

    filtered = [trip[CITY_IDX], trip[STATION_NAME_IDX], trip[YEAR_IDX]]
    channel.basic_publish(exchange="", routing_key="query2-pipe1", body=encode(filtered))
    logging.info(f"action: filter_callback | result: success | msg: published trip filtered {filtered}")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

logging.info(
    f"action: counter | result: in_progress | msg: start consuming from trip-start-station-topic ")

channel.start_consuming()
