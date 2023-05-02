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
time.sleep(5)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='raw_weather_data', durable=True)
channel.exchange_declare(exchange='weather_topic', exchange_type='fanout')

DATE_INDEX = 0
PRECTOT_INDEX = 1

EOF = "#"


def filter_weather(ch, method, properties, body):
    """
        input:  [ CITY, DATE, PRECTOT, QV2M, RH2M, PS, T2M_RANGE, TS, ... ]
        output: [ CITY, DATE, PRECTOT ]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if reg == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.")
        channel.basic_publish(exchange="weather_topic", routing_key='', body=encode(reg))
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    filtered = [reg[DATE_INDEX], reg[PRECTOT_INDEX]]

    channel.basic_publish(exchange="weather_topic", routing_key='', body=encode(filtered))
    logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success ")


channel.basic_consume(queue="raw_weather_data", on_message_callback=filter_weather, auto_ack=False)

logging.info(f"action: consuming | result: in_progress ")

channel.start_consuming()

logging.info(f"action: consuming | result: done ")
