from .rabbit_interface import RabbitInterface
from .messaging_protocol import *
from .eof import EOF
from .result import Result

from .server import ServerInterface

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

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
OP_CODE_QUERY = 7
OP_CODE_RESPONSE_QUERY1 = 8
OP_CODE_RESPONSE_QUERY2 = 9
OP_CODE_RESPONSE_QUERY3 = 10
OP_CODE_FINISH = 11
OP_CODE_WAIT = 12
OP_CODE_ERROR = 13
OP_CODE_EOF = 14

FINISH_MESSAGE = "Queries termiandas"

WEATHER_FIELDS = 22
STATION_FIELDS = 7
TRIPS_FIELDS = 9


class Analyzer(ServerInterface):
    """ Interface of the system that exposes the methods avaibles by the clients """

    def __init__(self):
        super().__init__()
        self.queries_ok = set()

    def handle_client(self, s):
        """ Handle a new client

        It waits for a command and data, process an action and responses to the client,
        indefinitely until the conection ends.

        """

        rabbit = RabbitInterface()

        while True:
            packet = receive(s)
            addr = s.getpeername()
            logging.debug(
                f'action: receive_message | result: success | ip: {addr[0]} | msg: {packet.get()}')

            if packet.opcode == OP_CODE_PING:

                send(s, Packet.new(OP_CODE_PONG, "Pong!"))
                logging.info(f'action: ping | result: success | client: {addr[0]} | msg: Ponged successfully.')

            elif packet.opcode == OP_CODE_INGEST_WEATHER:

                data = packet.get()
                result = self.ingest_weather(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling weathers"))
                    logging.error(
                        f'action: ingest_weather | result: fail | client: {addr[0]} | msg: Error in the ingestion of weathers.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_weather | result: success | client: {addr[0]} | msg: Weathers ingested correctly.')

            elif packet.opcode == OP_CODE_INGEST_STATIONS:

                data = packet.get()
                result = self.ingest_stations(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling stations"))
                    logging.error(
                        f'action: ingest_stations | result: fail | client: {addr[0]} | msg: Error in the ingestion of stations.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_stations | result: success | client: {addr[0]} | msg: Stations ingested correctly.')

            elif packet.opcode == OP_CODE_INGEST_TRIPS:

                data = packet.get()
                result = self.ingest_trips(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling trips"))
                    logging.error(
                        f'action: ingest_trips | result: fail | client: {addr[0]} | msg: Error in the ingestion of trips.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_strips | result: success | client: {addr[0]} | msg: trips ingested correctly.')

            elif packet.opcode == OP_CODE_QUERY:

                data = packet.get()
                try:
                    logging.info(f'action: handle_queries | result: starting')
                    opcode, result = self.get_query_results(rabbit)
                except Exception as e:
                    opcode, result = OP_CODE_WAIT, "para wacaha"
                    logging.error(
                        f'action: handle_queries | result: fail | msg: {e}')
                finally:
                    logging.info(
                        f'action: handle_queries | result: finished')

                if opcode is None or result is None:
                    send(s, Packet.new(OP_CODE_ERROR, "No seras muy fantaseosa vos?"))
                    logging.error(
                        f'action: handle_querys | result: fail | client: {addr[0]} | msg: {result}.')
                else:
                    send(s, Packet.new(opcode, result))
                    logging.info(
                        f'action: handle_querys | result: success | client: {addr[0]} | msg: {result}')
            elif packet.opcode == OP_CODE_EOF:
                data = packet.get()
                self.sendEOF(data, rabbit)
                send(s, Packet.new(OP_CODE_ACK, "joya"))

            elif packet.opcode == OP_CODE_ZERO:
                logging.info(f'action: disconnected | result: success | ip: {addr[0]}')
                break

        self.queries_ok.clear()
        rabbit.disconnect()

    def sendEOF(self, archivo, rabbit: RabbitInterface):
        """ Sends a EOF msg of the specified file ingestion """

        eof = EOF("start", "server")

        if archivo == "weathers":
            eof.source_id = "raw_weather_data"
            rabbit.publish_queue("raw_weather_data", eof.encode(), headers={"original": "true"})

        elif archivo == "stations":
            eof.source_id = "raw_station_data"
            rabbit.publish_queue("raw_station_data", eof.encode(), headers={"original": "true"})

        elif archivo == "trips":
            eof.source_id = "raw_trip_data"
            rabbit.publish_queue("raw_trip_data", eof.encode(), headers={"original": "true"})
        else:
            return

        logging.info(f'action: sending EOF | result: success | file: {archivo}')

    def ingest_weather(self, data, rabbit: RabbitInterface):
        """ Ingest the weather information """

        # Cantidad de weathers
        batch_size = int(data.pop(0))

        try:
            for i in range(batch_size):
                reg = data[i * WEATHER_FIELDS + 1:(i + 1) * WEATHER_FIELDS]
                rabbit.publish_queue("raw_weather_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_wheather | result: fail | msg: {e}')
            return False

        return True

    def ingest_stations(self, data, rabbit: RabbitInterface):
        """ Ingest the stations information """

        batch_size = int(data.pop(0))

        try:

            for i in range(batch_size):
                reg = data[i * STATION_FIELDS + 1:(i + 1) * STATION_FIELDS]
                rabbit.publish_queue("raw_station_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_stations | result: fail | msg: {e}')
            return False

        return True

    def ingest_trips(self, data, rabbit: RabbitInterface):
        """ Ingest the trips data """

        batch_size = int(data.pop(0))

        try:
            for i in range(batch_size):
                reg = data[i * TRIPS_FIELDS + 1:(i + 1) * TRIPS_FIELDS]
                rabbit.publish_queue("raw_trip_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_stations | result: fail | msg: {e}')
            return False

        return True

    def get_query_results(self, rabbit):
        """ Checks if exist results of any query and forward them to the client.
            If there is no results in the queue, the client is notified with an OP_CODE_WAIT
        """

        msg = rabbit.get("query_results")

        if not msg:
            return OP_CODE_WAIT, "para wacha"

        if EOF.is_eof(msg.decode()):
            stage_ok = msg.decode().split(".")[1]
            self.queries_ok.add(stage_ok)
            if len(self.queries_ok) < 3:
                return OP_CODE_WAIT, "para wacha"

            logging.info(f'action: get_query | result: success | msg: EOF {msg}')
            return OP_CODE_FINISH, FINISH_MESSAGE

        result = Result.decode(msg)

        if result.query == Result.QUERY_1:
            return OP_CODE_RESPONSE_QUERY1, result.data

        elif result.query == Result.QUERY_2:
            return OP_CODE_RESPONSE_QUERY2, result.data

        elif result.query == Result.QUERY_3:
            return OP_CODE_RESPONSE_QUERY3, result.data

        else:
            logging.error(f'action: get_query | result: fail | query: {result.query} | result: {result.data}')
