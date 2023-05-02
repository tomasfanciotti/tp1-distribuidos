#!/usr/bin/env python3
import pika
import time
import os
import logging
# noinspection PyUnresolvedReferences
from messaging_protocol import decode, encode       # module provided on the container


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

# Collect from station topic
result = channel.queue_declare(queue='', durable=True)
station_queue_name = result.method.queue
channel.queue_bind(exchange='station_topic', queue=station_queue_name)

# Collect from trip topic
result = channel.queue_declare(queue='', durable=True)
trip_queue_name = result.method.queue
channel.queue_bind(exchange='trip_topic', queue=trip_queue_name)

# Publish to topics
channel.exchange_declare(exchange="trip-start-station-topic", exchange_type="fanout")
channel.exchange_declare(exchange="trip-end-station-topic", exchange_type="fanout")

# station file
CITY_ID = 0
STATION_ID = 1
STATION_YEAR_ID = 5

# Trip file
TRIP_CITY_ID = 0
START_STATION = 2
END_STATION = 3
TRIP_YEAR_ID = 5

station_info = {}

EOF = "#"


def config_station(ch, method, properties, body):
    """
        input:  [ CITY, CODE, NAME, LATITUDE, LONGITUDE, YEARID ]
        output: None
    """

    station = decode(body)
    logging.info(f"action: config_callback | result: in_progress | body: {station} ")

    if station == EOF:
        logging.info(f"action: config_callback | result: success | msg: END OF FILE station.")
        ch.stop_consuming()
        return

    key = (station[CITY_ID], station[STATION_ID], station[STATION_YEAR_ID])
    value = [station[i] for i in range(len(station)) if i != STATION_ID and i != STATION_YEAR_ID and i != CITY_ID]

    if key in station_info:
        logging.warning(f"action: config_callback | result: warning | msg: key {key} already station info. Overwriting")

    station_info[key] = value
    logging.info(f"action: config_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):

    """
        input:  [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR]
        output1: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, START_NAME, START_LATITUDE, START_LONGITUDE ]
        output2: [ CITY, START_DATE, START_STATION, END_STATION, DURATION, YEAR, END_DATENAME, END_LATITUDE, END_LONGITUDE ]
    """

    trip = decode(body)
    logging.info(f"action: join_callback | result: in_progress | msg: {trip} ")

    if trip == EOF:
        logging.info(f"action: filter_callback | result: success | msg: END OF FILE trips.") 
        channel.basic_publish(exchange="trip-start-station-topic",  routing_key='', body=encode(trip))
        channel.basic_publish(exchange="trip-end-station-topic",  routing_key="", body=encode(trip))
        return

    trip_start_station = (trip[CITY_ID], trip[START_STATION], trip[TRIP_YEAR_ID])
    trip_end_station = (trip[CITY_ID], trip[END_STATION], trip[TRIP_YEAR_ID])

    if trip_start_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip STARTED in {trip_start_station}. Ignoring join..")
        return

    if trip_end_station not in station_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No station info found for trip ENDED in {trip_end_station}. Ignoring join..")
        return

    station = station_info[trip_start_station]
    joined = trip + station
    logging.info(f"action: join_callback | result: in_progress | msg: trip and START station joined -> {joined} ")

    channel.basic_publish(exchange="trip-start-station-topic", routing_key="", body=encode(joined))
    logging.info(f"action: join_callback | result: success | msg: published join in topic")

    station = station_info[trip_end_station]
    joined = trip + station
    logging.info(f"action: join_callback | result: in_progress | msg: trip and END station joined -> {joined} ")

    channel.basic_publish(exchange="trip-end-station-topic", routing_key="", body=encode(joined))
    logging.info(f"action: join_callback | result: success | msg: published join in topic")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=station_queue_name, on_message_callback=config_station, auto_ack=True)

logging.info(f"action: consuming stations | result: in_progress ")
channel.start_consuming()

logging.info(f"action: consuming trips | result: in_progress ")
channel.basic_consume(
    queue=trip_queue_name, on_message_callback=joiner, auto_ack=True)
channel.start_consuming()
