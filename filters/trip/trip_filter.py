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

channel.queue_declare(queue='raw_trip_data', durable=True)
channel.exchange_declare(exchange='trip_topic', exchange_type='fanout')

CITY_INDEX = 0
START_DATE_INDEX = 1
START_STATION_INDEX = 2
END_DATE_INDEX = 3
END_STATION_INDEX = 4
DURATION_INDEX = 5
MEMBER_INDEX = 6
YEAR_INDEX = 7

EOF = "#"


def filter_trip(ch, method, properties, body):
    """
        input:  [ CITY, START_DATE, START_STATION, END_DATE, END_STATION, DURATION, MEMBER, YEAR]
        output: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
    """

    reg = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | body: {reg} ")

    if reg == EOF:
        logging.info(f"action: filter_callback | result: done | msg: END OF FILE trips.")
        ch.basic_publish(exchange="trip_topic", routing_key='', body=encode(reg))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if reg[DURATION_INDEX][1:].replace(".", "").isdigit():

        if float(reg[DURATION_INDEX]) < 0:
            reg[DURATION_INDEX] = "0"

        filtered = [reg[CITY_INDEX], reg[START_DATE_INDEX], reg[START_STATION_INDEX], reg[END_STATION_INDEX], reg[DURATION_INDEX],
                    reg[YEAR_INDEX]]

        channel.basic_publish(exchange="trip_topic", routing_key='', body=encode(filtered))

        logging.info(f"action: filter_callback | result: in_progress | filtered: {filtered} ")
    else:
        logging.error(f"action: filter_callback | result: fail | ignored due type error | data: {reg} ")

    channel.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f"action: filter_callback | result: success.")


channel.basic_consume(queue="raw_trip_data", on_message_callback=filter_trip, auto_ack=False)

logging.info(f"action: consuming | result: in_progress ")

channel.start_consuming()

logging.info(f"action: consuming | result: done ")
