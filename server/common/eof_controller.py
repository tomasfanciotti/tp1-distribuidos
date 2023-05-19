from eof import EOF
from rabbit_interface import RabbitInterface
from messaging_protocol import decode, encode

EOF_MANAGER_QUEUE = "EOF_queue"


class EOFController(RabbitInterface):
    """ Module with the same functions of a RabbitInterface that it adds the logic
     needed to receive EOFs, detect and forward them to EOFManager.

     Also, this controller ensures the registration in the EOFManager so he can know that exists"""


    def __init__(self, stage, node, on_eof=None, stop_on_eof=False):
        super().__init__()
        self.stop_on_eof = stop_on_eof
        self.callback_on_eof = on_eof
        self.stage = stage
        self.node = node
        self.callback = None

        # Add node to manager
        self.publish_queue(EOF_MANAGER_QUEUE, EOF.create_register(stage, node).encode())

    def set_stage(self, stage):
        self.stage = stage

        # Add node to manager
        self.publish_queue(EOF_MANAGER_QUEUE, EOF.create_register(stage, self.node).encode())

    def set_on_eof(self, callback, stop=False):
        self.callback_on_eof = callback
        self.stop_on_eof = stop

    def send_EOF(self, src, original):
        """ Send a EOF to the EOFManager"""

        self.publish_queue(EOF_MANAGER_QUEUE, EOF(self.stage, self.node, src).encode(),
                           headers={"original": original})

    def consume_queue(self, queue, callback, auto_ack=False):
        self.callback = callback
        self.auto_ack = auto_ack
        super().consume_queue(queue, self.__callback, auto_ack)

    def consume_topic(self, callback, dest='default', auto_ack=False):
        self.callback = callback
        self.auto_ack = auto_ack
        super().consume_topic(self.__callback, dest, auto_ack)

    def __callback(self, ch, method, prop, msg):
        """ Callback used to process the incoming messages that checks if the message is a EOF or not.
        In case of EOF, executes the configured tasks by the user and sends EOF to Manager.
        Otherwise executes the original user's callback
        """

        if EOF.is_eof(decode(msg)):

            src = method.exchange + method.routing_key
            original = prop.headers.get("original") if prop.headers else "false"

            if self.stop_on_eof:
                ch.stop_consuming()
            if self.callback_on_eof:
                self.callback_on_eof(ch, method, prop, msg)

            self.send_EOF(src, original)
            if not self.auto_ack:
                ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            self.callback(ch, method, prop, msg)
