
from .messaging_protocol import encode
import time
import pika


def connectRabbit():

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))

    return connection


def init():

    result = False
    while not result:
        try:
            conn = connectRabbit()
            channel = conn.channel()
            channel.queue_declare(queue='raw_weather_data', durable=True)
            channel.queue_declare(queue='raw_station_data', durable=True)
            channel.queue_declare(queue='raw_trip_data', durable=True)
            conn.close()
            result = True

        except:
            time.sleep(10)
            print("Trying to connect to rabbit..")


# Wait for rabbitmq to come up
time.sleep(10)
init()

WEATHER_FIELDS = 21
STATION_FIELDS = 6
TRIPS_FIELDS = 8

EOF = "#"

def sendEOF(archivo):

    conn = connectRabbit()
    channel = conn.channel()

    queue = f"raw_{archivo.lower()}_data"
    channel.basic_publish(exchange="", routing_key=queue, body=encode(EOF))

    conn.close()


def handle_wheather(data):

    # Cantidad de weathers
    batch_size = int(data.pop(0))

    try:
        conn = connectRabbit()
        channel = conn.channel()

        for i in range(batch_size):
            reg = data[i*WEATHER_FIELDS+1:(i+1)*WEATHER_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_weather_data", body=encode(reg))

        conn.close()

    except Exception as e:

        return False

    return True


def handle_stations(data):

    batch_size = int(data.pop(0))

    try:

        conn = connectRabbit()
        channel = conn.channel()

        for i in range(batch_size):
            reg = data[i*STATION_FIELDS+1:(i+1)*STATION_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_station_data", body=encode(reg))

        conn.close()
    except Exception as e:

        return False

    return True


def handle_trips(data):

    batch_size = int(data.pop(0))

    try:
        conn = connectRabbit()
        channel = conn.channel()

        for i in range(batch_size):
            reg = data[i*TRIPS_FIELDS+1:(i+1)*TRIPS_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_trip_data", body=encode(reg))

        conn.close()

    except Exception as e:

        return False

    return True


def handle_query_1():
    return None