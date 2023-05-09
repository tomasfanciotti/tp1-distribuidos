import json
from rabbit_interface import RabbitInterface
from messaging_protocol import *
from eof import EOF
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="DEBUG",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

CONFIG_FILE = "config_manager.json"


def load_config():
    result = {}
    with open(CONFIG_FILE) as f:
        config = json.load(f)

    if "stages" in config:

        for stage in config["stages"]:
            result[stage["name"]] = {"source": stage["source"],
                                     "response": stage["response"],
                                     "replicas": stage["replicas"]
                                     }

    logging.info(f'action: load_config | result: success | config: {result}')
    return result


def handler(ch, method, properties, body):

    msg = decode(body)
    logging.info(f'action: handle_msg | result: in_progress | msg: {msg}')

    splitted = msg.split(".")
    opcode, params = int(splitted[0]), splitted[1:]

    if EOF.is_reg(msg):

        stage, node = params[0], params[1]
        if not stage in listeners:
            listeners[stage] = {}

        if node not in listeners[stage]:
            # logging.info(f'action: handle_opcode | result: warning | client: {addr[0]} | msg: overwrited setup for node {node} in stage {stage}.')
            listeners[stage][node] = {"EOF": False}
        else:
            logging.info(
                f'action: handle_register | result: warning | msg: node {node} already suscribed in stage {stage}')

        logging.info(f'action: handle_register | result: success | client: {node} | stage: {stage}')
        logging.debug(f'action: handle_register | result: success | stage: {stage} | listeners: {listeners[stage]}')

    elif EOF.is_eof(msg):

        stage, notifier = params
        original = properties.headers.get("original")
        logging.info(f'action: handle_eof | result: in_progress | stage: {stage} | node: {notifier}')

        source_type = config[stage]["source"]["type"]
        if source_type == "queue" and original == "true":
            for node in listeners[stage]:
                logging.debug(f'action: pushing EOF | result: in_progress | stage: {stage} | node: {node}')
                if node == notifier: continue
                rabbit.publish_queue(config[stage]["source"]["name"], encode(EOF), headers={"original": False})
                logging.debug(f'action: handle_eof | result: in_progress | msg: forwarding EOF to {node}')

        listeners[stage][notifier]["EOF"] = True

        logging.info(f'action: handle_eof | result: in_progress | msg: checking stage EOF')
        check_all_eof(stage, rabbit)

    else:
        logging.info(f'action: handle_msg | result: fail | msg: no recognized opcode {opcode} with params {params}')

    logging.debug(f'action: handle_msg | result: done')
    ch.basic_ack(delivery_tag=method.delivery_tag)


def check_all_eof(stage_name, rabbit: RabbitInterface):

    all_eof = True
    for data in listeners[stage_name].values():
        all_eof &= data["EOF"]

    if all_eof:

        response_type = config[stage_name]["response"]["type"]
        output_name = config[stage_name]["response"]["name"]
        eof = EOF(stage_name, "manager").encode()

        if response_type == "topic":
            rabbit.publish_topic(output_name, eof, headers={"original": True})
            logging.info(f'action: check_all_eof | result: in_progress | msg: published EOF in topic {output_name}')

        elif response_type == "queue":
            rabbit.publish_queue(output_name, eof, headers={"original": True})
            logging.info(f'action: check_all_eof | result: in_progress | msg: published EOF in queue {output_name}')

        else:
            logging.info(f'action: check_all_eof | result: fail | msg: unexpected response type {response_type}')
            return

    logging.info(f'action: check_all_eof | result: success | stage: {stage_name} | all_eof: {all_eof}.')


# Status
config = load_config()
listeners = {}

rabbit = RabbitInterface()

logging.info(f'action: eof_manager | result: in_progress | msg: start listening from EOF_queue.')
rabbit.consume_queue("EOF_queue", handler)

logging.info(f'action: eof_manager | result: done')
