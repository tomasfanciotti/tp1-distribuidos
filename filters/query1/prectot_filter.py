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

channel.exchange_declare(exchange='trip-weather-topic', exchange_type='fanout')

# Collect joined trips
result = channel.queue_declare(queue='', durable=True)
queue_name = result.method.queue
channel.queue_bind(exchange='trip-weather-topic', queue=queue_name)

# Publish filtered
channel.queue_declare(queue='query1-pipe1', durable=True)

PRECTOT_TRESHOLD = 20
EOF = "#"

def callback(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, PRECTOT ]
        output: [ PRECTOT ]
    """
    trip = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE trips.") 
        channel.basic_publish(exchange="",  routing_key='query1-pipe1', body=encode(trip))
        return
    
    if float(trip[-1]) > PRECTOT_TRESHOLD:
        channel.basic_publish(exchange='', routing_key='query1-pipe1', body=encode(trip))
        logging.info(f"action: filter_callback | result: success | msg: condition met. Sending to the next stage.")
    else:
        logging.info(f"action: filter_callback | result: success | msg: trip filtered.")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
