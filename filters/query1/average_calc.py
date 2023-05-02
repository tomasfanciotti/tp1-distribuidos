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
channel.queue_declare(queue='query1-pipe1', durable=True)
channel.queue_declare(queue='query1-pipe2', durable=True)

DURATION_INDEX = 3
EOF = "#"

status = {"trips": 0,
          "duration_sum": 0}


def callback(ch, method, properties, body):
    """
        input:  [ PRECTOT ]
        output: [ AVERAGE ]
    """
    trip = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE trips.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        return

    status["trips"] += 1
    status["duration_sum"] += float(trip[DURATION_INDEX])

    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(
        f"action: filter_callback | result: success | trips: {status['trips']} | duration_sum : {status['duration_sum']} ")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='query1-pipe1', on_message_callback=callback, auto_ack=False)

channel.start_consuming()

if status["trips"] == 0:
    logging.info(
        f"action: response | result: fail | trips: {status['trips']} | duration_sum : {status['duration_sum']} | msg: no trips loaded")

else:

    result = str(round(status["duration_sum"] / status["trips"], 4))

    channel.basic_publish(exchange="",
                          routing_key="query1-pipe2",
                          body=encode(result))
    logging.info(
        f"action: response_enqueue | result: success | result: {result} | trips: {status['trips']} ")