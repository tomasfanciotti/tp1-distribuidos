

# Dataclass for the end of file

class EOF:

    OP_CODE_EOF = 1
    OP_CODE_REGISTER = 2

    def __init__(self, stage, node ):
        """ Generates a EOF struct with specified stage and node """

        self.opcode = self.OP_CODE_EOF
        self.stage = stage
        self.node = node

    @classmethod
    def create_register(cls, stage, node):
        """" Generates a REGISTER struct with specified stage and node to send to Manager """

        reg = EOF(stage,node)
        reg.opcode = cls.OP_CODE_REGISTER
        return reg

    @classmethod
    def is_eof(cls, msg: str):
        """ Check if specified string is a EOF encoded """

        if not isinstance(msg, str):
            return False

        eof = msg.split(".")
        return len(eof) == 3 and int(eof[0]) == cls.OP_CODE_EOF

    @classmethod
    def is_reg(cls, msg):
        """ Check if specified string is a Register encoded """

        if not isinstance(msg, str):
            return False

        eof = msg.split(".")
        return len(eof) == 3 and int(eof[0]) == cls.OP_CODE_REGISTER

    @classmethod
    def decode(cls, msg):
        """ Decode a EOF encoded """

        if not cls.is_eof(msg):
            return None

        splitted = msg.decode().split(".")
        return EOF(stage=splitted[1], node=splitted[2])

    def encode(self):
        """ Encodes a EOF struct to be sent to rabbit """

        return f"{self.opcode}.{self.stage}.{self.node}".encode()


def send_EOF(stage, node, properties, rabbit):

    if properties.headers is None:
        result, msg = "fail", "no encabzado detectado"
    else:
        original = properties.headers['original']
        rabbit.publish_queue("EOF_queue", EOF(stage, node).encode(),
                             headers={"original": original})
        result, msg = "success", "END OF FILE trip"

    return result, msg


def add_listener(stage, node, rabbit):

    rabbit.publish_queue("EOF_queue", EOF.create_register(stage, node).encode())
