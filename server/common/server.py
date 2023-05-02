import socket
import logging
import signal
from .messaging_protocol import Packet, receive, send
from .analyzer import handle_wheather, handle_stations, handle_trips, handle_query_1, sendEOF, connectRabbit
import concurrent
from concurrent.futures import ThreadPoolExecutor


class Server:
    def __init__(self, port, listen_backlog, handler):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.listening = True
        signal.signal(signal.SIGTERM, self.__stop_listening)
        self.handler = handler

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
            self.handler(client_sock)
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
