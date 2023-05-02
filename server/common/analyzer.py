
from .messaging_protocol import *
import time
import pika

# Excepcionales
OP_CODE_CORRUPTED_REQUEST = -1
OP_CODE_ZERO = 0

# OpCodes
OP_CODE_PING = 1
OP_CODE_PONG = 2
OP_CODE_INGEST_WEATHER = 3
OP_CODE_INGEST_STATIONS = 4
OP_CODE_INGEST_TRIPS = 5
OP_CODE_ACK = 6
OP_CODE_QUERY1 = 7
OP_CODE_RESPONSE_QUERY1 = 8
OP_CODE_ERROR = 9
OP_CODE_EOF = 10

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


def handler(client_sock):

    connection = connectRabbit()
    channel = connection.channel()

    while True:
        packet = receive(client_sock)
        addr = client_sock.getpeername()
        logging.debug(
            f'action: receive_message | result: success | ip: {addr[0]} | msg: {packet.get()}')

        if packet.opcode == OP_CODE_PING:

            send(client_sock, Packet.new(OP_CODE_PONG, "Pong!"))
            logging.info(f'action: ping | result: success | client: {addr[0]} | msg: Ponged successfully.')

        elif packet.opcode == OP_CODE_INGEST_WEATHER:

            data = packet.get()
            result = handle_wheather(data, channel)
            if not result:
                send(client_sock, Packet.new(OP_CODE_ERROR, "There was a problem handling weathers"))
                logging.error(f'action: ingest_weather | result: fail | client: {addr[0]} | msg: Error in the ingestion of weathers.')
            else:
                send(client_sock, Packet.new(OP_CODE_ACK, "ACK!"))
                logging.info(f'action: ingest_weather | result: success | client: {addr[0]} | msg: Weathers ingested correctly.')

        elif packet.opcode == OP_CODE_INGEST_STATIONS:

            data = packet.get()
            result = handle_stations(data, channel)
            if not result:
                send(client_sock, Packet.new(OP_CODE_ERROR, "There was a problem handling stations"))
                logging.error(f'action: ingest_stations | result: fail | client: {addr[0]} | msg: Error in the ingestion of stations.')
            else:
                send(client_sock, Packet.new(OP_CODE_ACK, "ACK!"))
                logging.info(f'action: ingest_stations | result: success | client: {addr[0]} | msg: Stations ingested correctly.')

        elif packet.opcode == OP_CODE_INGEST_TRIPS:

            data = packet.get()
            result = handle_trips(data, channel)
            if not result:
                send(client_sock, Packet.new(OP_CODE_ERROR, "There was a problem handling trips"))
                logging.error(f'action: ingest_trips | result: fail | client: {addr[0]} | msg: Error in the ingestion of trips.')
            else:
                send(client_sock, Packet.new(OP_CODE_ACK, "ACK!"))
                logging.info(f'action: ingest_strips | result: success | client: {addr[0]} | msg: trips ingested correctly.')

        elif packet.opcode == OP_CODE_QUERY1:

            data = packet.get()
            result = handle_query_1()
            if result is None:
                send(client_sock, Packet.new(OP_CODE_ERROR, "No seras muy fantaseosa vos?"))
                logging.error(f'action: query #1 | result: fail | client: {addr[0]} | msg: Error in the retreival of query #1.')
            else:
                send(client_sock, Packet.new(OP_CODE_RESPONSE_QUERY1, result))
                logging.info(f'action: query #1 | result: success | client: {addr[0]} | msg: query #1 retreived correctly.')
        elif packet.opcode == OP_CODE_EOF:
            data = packet.get()
            sendEOF(data)
            send(client_sock, Packet.new(OP_CODE_ACK, "joya"))

        elif packet.opcode == OP_CODE_ZERO:
            logging.info(f'action: disconnected | result: success | ip: {addr[0]}')
            break

    connection.close()


# Wait for rabbitmq to come up
time.sleep(10)
init()

WEATHER_FIELDS = 22
STATION_FIELDS = 7
TRIPS_FIELDS = 9

EOF = "#"

def sendEOF(archivo):

    conn = connectRabbit()
    channel = conn.channel()

    channel.basic_publish(exchange="", routing_key="raw_weather_data", body=encode(EOF))
    channel.basic_publish(exchange="", routing_key="raw_station_data", body=encode(EOF))
    channel.basic_publish(exchange="", routing_key="raw_trip_data", body=encode(EOF))

    conn.close()


def handle_wheather(data, channel):

    # Cantidad de weathers
    batch_size = int(data.pop(0))

    try:
        for i in range(batch_size):
            reg = data[i*WEATHER_FIELDS+1:(i+1)*WEATHER_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_weather_data", body=encode(reg))


    except Exception as e:

        return False

    return True


def handle_stations(data, channel):

    batch_size = int(data.pop(0))

    try:

        for i in range(batch_size):
            reg = data[i*STATION_FIELDS+1:(i+1)*STATION_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_station_data", body=encode(reg))

    except Exception as e:

        return False

    return True


def handle_trips(data, channel):

    batch_size = int(data.pop(0))

    try:

        for i in range(batch_size):
            reg = data[i*TRIPS_FIELDS+1:(i+1)*TRIPS_FIELDS]
            channel.basic_publish(exchange="", routing_key="raw_trip_data", body=encode(reg))

    except Exception as e:

        return False

    return True


def handle_query_1():
    return None