class Batching:

    BATCH_SIZE = 10

    def __init__(self, rabbit):
        self.rabbit = rabbit
        self.buffer = {"topics": {}, "queues": {}}
        self.callback = None

    def publish_batch_to_topic(self, topic, msg, routing_key="", headers=None):

        key = (topic, routing_key, headers)

        # Check if exists
        if key not in self.buffer["topics"]:
            self.buffer["topics"][key] = []

        # Append
        self.buffer["topics"][key].append(msg)

        # Send if size met condition
        if len(self.buffer["topics"][key]) >= Batching.BATCH_SIZE:
            encoded = b"%".join(self.buffer["topics"][key])
            self.rabbit.publish_topic(topic, encoded, routing_key, headers)
            self.buffer["topics"][key] = []

    def publish_batch_to_queue(self, queue, msg, headers=None):

        key = (queue, headers)

        # Check if exists
        if key not in self.buffer["queues"]:
            self.buffer["queues"][key] = []

        # Append
        self.buffer["queues"][key].append(msg)

        # Send if size met condition
        if len(self.buffer["queues"][key]) >= Batching.BATCH_SIZE:

            print(f"Pushing: {len(self.buffer['queues'][key])} messages")

            encoded = b"%".join(self.buffer["queues"][key])
            self.rabbit.publish_queue(queue, encoded, headers)
            self.buffer["queues"][key] = []
            print(encoded)

    def push_buffer(self):

        for t, r, h in self.buffer["topics"]:
            encoded = b"%".join(self.buffer["queues"][(t, r, h)])
            if len(encoded):
                self.rabbit.publish_topic(t, encoded, r, h)
            print(f"Flushing to topic {t}: {len(self.buffer['queues'][(t, r, h)])} messages")

        for q, h in self.buffer["queues"]:

            encoded = b"%".join(self.buffer["queues"][(q, h)])
            if len(encoded):
                self.rabbit.publish_queue(q, encoded, h)
            print(f"Flushing to queue {q}: {len(self.buffer['queues'][(q, h)])} messages")

    def consume_batch_queue(self, queue, callback, auto_ack=False):
        self.callback = callback
        self.auto_ack = auto_ack
        self.rabbit.consume_queue(queue, self.__callback, auto_ack)

    def consume_batch_topic(self, callback, dest='default', auto_ack=False):
        self.callback = callback
        self.auto_ack = auto_ack
        self.rabbit.consume_topic(self.__callback, dest, auto_ack)

    def __callback(self, ch, method, prop, msg):

        if self.callback is None:
            print("Flaco tenes un problemita")

        for x in msg.split(b'%'):
            self.callback(ch, method, prop, x)

        if not self.auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)