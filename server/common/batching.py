class Batching:
    """ Batching is a class that uses a RabbitInterface to send and recevive batches of data.
     It has a buffer by each topic/queue and flushes to the original destination when the BATCH_SIZE has
     reached or when the sender decides to do it"""

    BATCH_SIZE = 10

    def __init__(self, rabbit):
        self.rabbit = rabbit
        self.buffer = {"topics": {}, "queues": {}}
        self.callback = None
        self.auto_ack = False

    def publish_batch_to_topic(self, topic, msg, routing_key="", headers=None):
        """ Store the msg to the buffer and sends it to the specified topic when
         len(buffer) reaches the limit. """

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
        """ Store the msg to the buffer and sends it to the specified queue when
         len(buffer) reaches the limit. """

        key = (queue, headers)

        # Check if exists
        if key not in self.buffer["queues"]:
            self.buffer["queues"][key] = []

        # Append
        self.buffer["queues"][key].append(msg)

        # Send if size met condition
        if len(self.buffer["queues"][key]) >= Batching.BATCH_SIZE:

            encoded = b"%".join(self.buffer["queues"][key])
            self.rabbit.publish_queue(queue, encoded, headers)
            self.buffer["queues"][key] = []


    def push_buffer(self):
        """ Sends all batches to its respective topic/queue. Empties all buffers"""

        for t, r, h in self.buffer["topics"]:
            encoded = b"%".join(self.buffer["topics"][(t, r, h)])
            if len(encoded):
                print(f"Flushing to topic {t}: {len(self.buffer['topics'][(t, r, h)])} messages")
                self.rabbit.publish_topic(t, encoded, r, h)
                self.buffer["topics"][(t, r, h)].clear()

        for q, h in self.buffer["queues"]:

            encoded = b"%".join(self.buffer["queues"][(q, h)])
            if len(encoded):
                print(f"Flushing to queue {q}: {len(self.buffer['queues'][(q, h)])} messages")
                self.rabbit.publish_queue(q, encoded, h)
                self.buffer["queues"][(q, h)].clear()

    def consume_batch_queue(self, queue, callback, auto_ack=False):
        """ Consume a batch recieved by a queue using __Callback """

        self.callback = callback
        self.auto_ack = auto_ack
        self.rabbit.consume_queue(queue, self.__callback, auto_ack)

    def consume_batch_topic(self, callback, dest='default', auto_ack=False):
        """ Consume a batch recieved by a topic using __Callback """

        self.callback = callback
        self.auto_ack = auto_ack
        self.rabbit.consume_topic(self.__callback, dest, auto_ack)

    def __callback(self, ch, method, prop, msg):
        """ Split the message (batch) by the delimitor and proccess all sub-messages with
        the callback specified by the user """

        if self.callback is None:
            print("No callback was provided.. Ignoring")

        for x in msg.split(b'%'):
            self.callback(ch, method, prop, x)

        if not self.auto_ack:
            ch.basic_ack(delivery_tag=method.delivery_tag)