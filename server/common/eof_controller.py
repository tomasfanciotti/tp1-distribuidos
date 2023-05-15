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


"""
    def publish_batch_to_topic(self, topic, msg, routing_key="", headers=None):

        key = (topic, routing_key, headers)

        # Check if exists
        if key not in self.buffer["topics"]:
            self.buffer["topics"][key] = []

        # Append
        self.buffer["topics"][key].append(msg)

        # Send if size met condition
        if len(self.buffer["topics"][key]) > EOFController.BATCH_SIZE:
            encoded = b"%".join(self.buffer["topics"][key])
            self.publish_topic(topic, encoded, routing_key, headers)
            self.buffer["topics"][key] = []

    def publish_batch_to_queue(self, queue, msg, headers=None):

        key = (queue, headers)

        # Check if exists
        if key not in self.buffer["queues"]:
            self.buffer["queues"][key] = []

        # Append
        self.buffer["queues"][key].append(msg)

        # Send if size met condition
        if len(self.buffer["queues"][key]) > EOFController.BATCH_SIZE:
            encoded = b"%".join(self.buffer["queues"][key])
            self.publish_queue(queue, encoded, headers)
            self.buffer["queues"][key] = []

    def __push_buffer(self):

        for t, r, h in self.buffer["topics"]:
            encoded = b"%".join(self.buffer["queues"][(t, r, h)])
            self.publish_topic(t, encoded, r, h)

        for q, h in self.buffer["queues"]:
            encoded = b"%".join(self.buffer["queues"][(q, h)])
            self.publish_topic(q, encoded, h)
"""
