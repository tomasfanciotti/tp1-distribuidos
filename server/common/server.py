import socket
import logging
import signal
import concurrent
from concurrent.futures import ThreadPoolExecutor


class ServerInterface:
    """ Class that contains the business flows.

    This class has a main method invoqued by the server on each new incoming connection. """

    def handle_client(self, s: socket.socket):
        pass


class Server:
    """ Server class responsible of open the listening socket at the configured port and receive new clients
    to handle the using a ServerInterface"""

    def __init__(self, port, listen_backlog, iface: ServerInterface):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.listening = True
        signal.signal(signal.SIGTERM, self.__stop_listening)
        self.iface = iface

    def __stop_listening(self, *args):
        self.listening = False
        self._server_socket.close()

    def run(self):

        while self.listening:
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)

            except OSError as e:
                logging.error(f"action: accept_connections | result: fail | error: {e}")

        logging.info(f"action: run | result: finished | msg: server shutting down ")


    def __handle_client_connection(self, client_sock):

        try:
            self.iface.handle_client(client_sock)
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
