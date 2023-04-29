from messaging_protocol import decode,encode
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

channel.queue_declare(queue='raw_trip_data', durable=True)
channel.exchange_declare(exchange='trip_topic', exchange_type='fanout')

START_DATE_INDEX = 0
START_STATION_INDEX = 1
END_DATE_INDEX = 2
END_STATION_INDEX = 3
DURATION_INDEX = 4
MEMBER_INDEX = 5
YEAR_INDEX = 6


def filter_trip(ch, method, properties, body):

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if reg[DURATION_INDEX][1:].replace(".", "").isdigit():

        if float(reg[DURATION_INDEX]) < 0:
            reg[DURATION_INDEX] = "0"

        filtered = [reg[START_DATE_INDEX], reg[START_STATION_INDEX], reg[END_STATION_INDEX], reg[DURATION_INDEX], reg[YEAR_INDEX]]

        channel.basic_publish(exchange="trip_topic",  routing_key='', body=encode(filtered))

        logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")
    else:
        logging.error(f"action: filter_callback | result: fail | ignored due type error | data: {reg} ")

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success.")


channel.basic_consume(queue="raw_trip_data", on_message_callback=filter_trip, auto_ack=False)

logging.info(f"action: consuming | result: in_progress ")

channel.start_consuming()

logging.info(f"action: consuming | result: done ")