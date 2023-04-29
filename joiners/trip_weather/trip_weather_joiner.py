#!/usr/bin/env python3
import pika
import time
import os
import logging
from messaging_protocol import encode, decode

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

# Collect from weather topic
result = channel.queue_declare(queue='', durable=True)
weather_queue_name = result.method.queue
channel.queue_bind(exchange='weather_topic', queue=weather_queue_name)

# Collect from trip topic
result = channel.queue_declare(queue='', durable=True)
trip_queue_name = result.method.queue
channel.queue_bind(exchange='trip_topic', queue=trip_queue_name)

# Collect from trip topic
channel.exchange_declare(exchange="trip-weather-topic", exchange_type="fanout")

weather_info = {}

# Weather file
DATE_INDEX = 0
PRECTOT_INDEX = 1

# Trip file
START_DATE_INDEX = 0

EOF = "#"


def config_weather(ch, method, properties, body):
    weather = decode(body)
    logging.info(f"action: filter_callback | result: in_progress | msg: {weather} ")

    if weather == EOF:
        ch.stop_consuming()
        return

    key, value = weather[DATE_INDEX].split(" ")[0], [weather[PRECTOT_INDEX]]

    if key in weather_info:
        logging.warning(f"action: filter_callback | result: warning | msg: key {key} already weather info. Overwriting")

    weather_info[key] = value
    logging.info(f"action: filter_callback | result: success | msg: sored '{value}' value in '{key}' key")


def joiner(ch, method, properties, body):
    trip = decode(body)
    logging.info(f"action: join_callback | result: in_progress | msg: {trip} ")

    trip_weather = trip[START_DATE_INDEX].split(" ")[0]
    if trip_weather not in weather_info:
        logging.warning(
            f"action: join_callback | result: warning | msg: No weather info found for trip started in {trip_weather}. Ignoring join..")
        return

    wheather = weather_info[trip_weather]
    joined = trip + wheather
    logging.info(f"action: join_callback | result: in_progress | msg: trip and weather joined -> {joined} ")

    channel.basic_publish(exchange="trip-weather-topic", routing_key="", body=encode(joined))
    logging.info(f"action: join_callback | result: success | msg: published join in topic")


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=weather_queue_name, on_message_callback=config_weather, auto_ack=True)

logging.info(f"action: consuming weathers | result: in_progress ")
channel.start_consuming()

logging.info(f"action: consuming trips | result: in_progress ")
channel.basic_consume(
    queue=trip_queue_name, on_message_callback=joiner, auto_ack=True)
channel.start_consuming()