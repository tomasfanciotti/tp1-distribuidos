
from .messaging_protocol import encode
import time
import pika

# Wait for rabbitmq to come up
time.sleep(10)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='raw_weather_data', durable=True)

WEATHER_FIELDS = 21
STATION_FIELDS = 6
TRIPS_FIELDS = 8


def handle_wheather(data):

    # Cantidad de weathers
    batch_size = int(data.pop(0))

    try:

        for i in range(batch_size):
            reg = data[i*WEATHER_FIELDS+1:(i+1)*WEATHER_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_weather_data", body=encode(reg))

    except Exception as e:

        return False

    return True


def handle_stations(data):

    batch_size = int(data.pop(0))

    try:

        for i in range(batch_size):
            reg = data[i*STATION_FIELDS+1:(i+1)*STATION_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_station_data", body=encode(reg))

    except Exception as e:

        return False

    return True


def handle_trips(data):

    batch_size = int(data.pop(0))

    try:

        with open("trips.csv", "a") as file:
            for i in range(batch_size):
                reg = ",".join(data[i*TRIPS_FIELDS+1:(i+1)*TRIPS_FIELDS])
                file.write(reg+"\n")

    except:

        return False

    return True


def handle_query_1():
    return None