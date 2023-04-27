import socket
import logging
import signal
from .messaging_protocol import Packet, decode, receive, send
from .analyzer import handle_wheather, handle_stations
import concurrent
from concurrent.futures import ThreadPoolExecutor


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
OP_CODE_ERROR = 7


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.listening = True
        signal.signal(signal.SIGTERM, self.__stop_listening)

    def __stop_listening(self, *args):
        self.listening = False
        self._server_socket.close()

    def run(self):

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            while self.listening:
                try:
                    client_sock = self.__accept_new_connection()
                    executor.submit(self.__handle_client_connection, client_sock)

                except OSError as e:
                    logging.error(f"action: accept_connections | result: fail | error: {e}")

        logging.info(f"action: run | result: finished | msg: server shutting down ")


    def __handle_client_connection(self, client_sock):

        try:
            while True:
                packet = receive(client_sock)
                addr = client_sock.getpeername()
                logging.debug(
                    f'action: receive_message | result: success | ip: {addr[0]} | msg: {packet.get()}')

                if packet.opcode == OP_CODE_PING:

                    send(client_sock, Packet.new(OP_CODE_PONG, "Pong!"))
                    logging.info(f'action: ping | result: success | client: {addr[0]} | msg: Ponged successfully.')

                if packet.opcode == OP_CODE_INGEST_WEATHER:

                    data = packet.get()
                    result = handle_wheather(data)
                    if not result:
                        send(client_sock, Packet.new(OP_CODE_ERROR, "There was a problem handling weathers"))
                        logging.error(f'action: ingest_weather | result: fail | client: {addr[0]} | msg: Error in the ingestion of weathers.')
                    else:
                        send(client_sock, Packet.new(OP_CODE_ACK, "ACK!"))
                        logging.info(f'action: ingest_weather | result: success | client: {addr[0]} | msg: Weathers ingested correctly.')

                if packet.opcode == OP_CODE_INGEST_STATIONS:

                    data = packet.get()
                    result = handle_stations(data)
                    if not result:
                        send(client_sock, Packet.new(OP_CODE_ERROR, "There was a problem handling stations"))
                        logging.error(f'action: ingest_stations | result: fail | client: {addr[0]} | msg: Error in the ingestion of stations.')
                    else:
                        send(client_sock, Packet.new(OP_CODE_ACK, "ACK!"))
                        logging.info(f'action: ingest_stations | result: success | client: {addr[0]} | msg: Stations ingested correctly.')

                elif packet.opcode == OP_CODE_ZERO:
                    logging.info(f'action: disconnected | result: success | ip: {addr[0]}')
                    break

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
