

# Dataclass for the query results

class Result:

    QUERY_1 = 1
    QUERY_2 = 2
    QUERY_3 = 3

    def __init__(self, query, data : str):
        """ Generates a Result struct with specified query and result data """

        self.query = query
        self.data = data

    @classmethod
    def query1(cls, data):
        """" Generates a Query1 Result struct with specified result data """
        return Result(cls.QUERY_1, cls.__check_data(data))

    @classmethod
    def query2(cls, data):
        """" Generates a Query2 Result struct with specified result data """
        return Result(cls.QUERY_2, cls.__check_data(data))

    @classmethod
    def query3(cls, data):
        """" Generates a Query3 Result struct with specified result data """
        return Result(cls.QUERY_3, cls.__check_data(data))

    @classmethod
    def decode(cls, msg: bytes):
        """ Decode a Result encoded from bytes"""
        print(msg)
        if not isinstance(msg, bytes):
            return None

        splitted = msg.decode().split(".")
        if not splitted[0].isdigit():
            return None

        query = int(splitted[0])
        if query not in (cls.QUERY_1, cls.QUERY_2, cls.QUERY_3):
            return None

        data = ".".join(splitted[1:])
        return Result(query=query, data=data)

    def encode(self):
        """ Encodes a Result struct to bytes """

        return f"{self.query}.{self.data}".encode()

    def __str__(self):
        return f"Query: {self.query} | Result: {self.data}"

    @classmethod
    def __check_data(cls, data):

        if isinstance(data, str):
            return data

        if isinstance(data, bytes):
            return data.decode()
