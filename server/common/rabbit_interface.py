import time
import pika
import signal


class RabbitInterface:
    """ Middleware that contains the configuration and comunication with RabbitMQ using Pika.

    - Defines the main methods to publish and consume of a queue or topics.
    - Allows to bind several annonymous queues to an one or more exchanges.
    - Handle the setup and releasing of resources.
    """

    def __init__(self):

        while True:
            try:
                self.conn = self.__connect()
                self.channel = self.conn.channel()
                break

            except:
                time.sleep(7)

        signal.signal(signal.SIGTERM, self.__stop_listening)
        self.running = True
        self.annon_q = {}
        self.init()

    def __connect(self):

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        return connection

    def init(self):
        """ Defines the queues and exchanges that the system will need"""

        # Data exchanges
        self.channel.exchange_declare(exchange='weather_topic', exchange_type='fanout')
        self.channel.exchange_declare(exchange='station_topic', exchange_type='fanout')
        self.channel.exchange_declare(exchange='trip_topic', exchange_type='fanout')

        # Join exchanges
        self.declare_and_bind('join1-stations-collector', 'station_topic')
        self.declare_and_bind('join1-trips-collector', 'trip_topic')
        self.declare_and_bind('join2-trips-collector', 'trip_topic')
        self.declare_and_bind('join2-weather-collector', 'weather_topic')
        self.channel.exchange_declare(exchange="trip-start-station-topic", exchange_type="fanout")
        self.channel.exchange_declare(exchange="trip-end-station-topic", exchange_type="fanout")
        self.channel.exchange_declare(exchange="trip-weather-topic", exchange_type="fanout")

        # Raw data Queues
        self.channel.queue_declare(queue='raw_weather_data', durable=True)
        self.channel.queue_declare(queue='raw_station_data', durable=True)
        self.channel.queue_declare(queue='raw_trip_data', durable=True)

        # Pipeline 1 Queues
        self.declare_and_bind('query1-collector', 'trip-weather-topic')
        self.channel.queue_declare(queue='query1-pipe1', durable=True)
        # self.channel.queue_declare(queue='query1-pipe2', durable=True)

        # Pipeline 2 Queues
        self.declare_and_bind('query2-collector', 'trip-start-station-topic')
        self.channel.queue_declare(queue='query2-pipe1', durable=True)
        # self.channel.queue_declare(queue='query2-pipe2', durable=True)

        # Pipeline 3 Queues
        self.declare_and_bind('query3-collector', 'trip-start-station-topic')
        self.declare_and_bind('query3-collector', 'trip-end-station-topic')
        self.channel.queue_declare(queue='query3-pipe1', durable=True)
        self.channel.queue_declare(queue='query3-pipe2', durable=True)
        self.channel.queue_declare(queue='query3-pipe3', durable=True)
        # self.channel.queue_declare(queue='query3-pipe4', durable=True)

        self.channel.queue_declare(queue='EOF_queue', durable=True)
        self.channel.queue_declare(queue='query_results', durable=True)

    def bind_topic(self, exchange, routing_key='', dest='default'):
        """ Binding of a exchange with a specific destination queue """

        if dest not in self.annon_q:
            self.annon_q[dest] = dest  # self.channel.queue_declare(queue='', durable=True).method.queue

        single_exchange = True
        if isinstance(exchange, list):
            single_exchange = False

        if single_exchange:
            self.channel.queue_bind(self.annon_q[dest], exchange=exchange, routing_key=routing_key)

        else:
            if not isinstance(exchange, dict):
                raise TypeError("routing_key, debe ser un diccionario")

            for ex in exchange:
                if ex not in routing_key:
                    continue

                self.channel.queue_bind(self.annon_q[dest], exchange=ex, routing_key=routing_key[ex])

    def publish_topic(self, topic, msg, routing_key="", headers=None):
        """ Publish a message to the specified topic with custom routing keys or headers"""

        if not self.running: return

        if headers is not None:
            self.channel.basic_publish(exchange=topic,
                                       routing_key=routing_key,
                                       body=msg,
                                       properties=pika.BasicProperties(headers=headers))
        else:
            self.channel.basic_publish(exchange=topic, routing_key=routing_key, body=msg)

    def consume_topic(self, callback, dest='default', auto_ack=False):
        """ Consume messages of a topic selecting the destination queue to read """

        if not self.running: return

        self.channel.basic_consume(
            queue=self.annon_q[dest], on_message_callback=callback, auto_ack=auto_ack)

        self.channel.start_consuming()

    def publish_queue(self, queue, msg, headers=None):
        """ Publish a message to the specified queue with custom headers"""

        if not self.running: return

        if headers is not None:
            self.channel.basic_publish(exchange="",
                                       routing_key=queue,
                                       body=msg,
                                       properties=pika.BasicProperties(headers=headers))
        else:
            self.channel.basic_publish(exchange="", routing_key=queue, body=msg)

    def consume_queue(self, queue, callback, auto_ack=False):
        """ Consume messages of a specific topic """

        if not self.running: return

        self.channel.basic_consume(
            queue=queue, on_message_callback=callback, auto_ack=auto_ack)

        self.channel.start_consuming()

    def get(self, queue):
        """ Get a message if the queue is not empty, otherwise will return none"""

        method_frame, header_frame, body = self.channel.basic_get(queue)
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
            return body

    def __stop_listening(self, *args):
        """ Gracefully shutdown """

        self.running = False
        self.channel.stop_consuming()

    def disconnect(self):
        """ Release owned resources and disconnect the session """

        for name in self.annon_q:
            self.channel.queue_delete(queue=self.annon_q[name])

        self.conn.close()

    def declare_and_bind(self, queue, exchange):
        self.channel.queue_declare(queue=queue, durable=True)
        self.channel.queue_bind(queue, exchange=exchange, routing_key="")
