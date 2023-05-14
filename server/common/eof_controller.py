from eof import EOF
from rabbit_interface import RabbitInterface
from messaging_protocol import decode, encode

EOF_MANAGER_QUEUE = "EOF_queue"


class EOFController(RabbitInterface):

    def __init__(self, stage, node, on_eof=None, stop_on_eof=False):
        super().__init__()
        self.stop_on_eof = stop_on_eof
        self.callback_on_eof = on_eof
        self.stage = stage
        self.node = node

        # Add node to manager
        self.publish_queue(EOF_MANAGER_QUEUE, EOF.create_register(stage, node).encode())

    def set_stage(self, stage):
        self.stage = stage

        # Add node to manager
        self.publish_queue(EOF_MANAGER_QUEUE, EOF.create_register(stage, self.node).encode())

    def set_on_eof(self, callback, stop=False ):
        self.callback_on_eof = callback
        self.stop_on_eof = stop

    def send_EOF(self, properties):

        if properties.headers is None:
            result, msg = "fail", "no encabzado detectado"
        else:
            original = properties.headers['original']
            self.publish_queue(EOF_MANAGER_QUEUE, EOF(self.stage, self.node).encode(),
                               headers={"original": original})

            result, msg = "success", "END OF FILE trip"

        return result, msg

    def consume_queue(self, queue, callback, auto_ack=False):
        self.callback = callback
        super().consume_queue(queue, self.__callback, auto_ack)

    def consume_topic(self, callback, dest='default', auto_ack=False):
        self.callback = callback
        super().consume_topic(self.__callback, dest, auto_ack)

    def __callback(self, ch, method, prop, msg):

        if EOF.is_eof(decode(msg)):

            self.send_EOF(prop)
            if self.stop_on_eof:
                ch.stop_consuming()
            if self.callback_on_eof:
                self.callback_on_eof(ch, method, prop, msg)

            result = True
        else:
            result = self.callback(ch, method, prop, msg)

        if result:
            ch.basic_ack(delivery_tag=method.delivery_tag)
