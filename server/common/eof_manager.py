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
EOF_MANAGER_QUEUE = "EOF_queue"

def load_config():
    result = {}
    with open(CONFIG_FILE) as f:
        config = json.load(f)

    if "stages" in config:

        for stage in config["stages"]:
            result[stage["name"]] = {"source": stage["source"],
                                     "response": stage["response"]
                                     }
            logging.info(f'action: load_config | stage: {stage["name"]} | '
                         f'source: {stage["source"]} | response: {stage["response"]}')

    return result


def check_error(stage, notifier, source):
    error = False

    if stage not in config:
        logging.error(f'action: checking | result: error | msg: no config for stage {stage}')
        error = True
    else:
        if source not in map(lambda x: x["name"], config[stage]["source"]):
            logging.error(f'action: checking | result: error | msg: no source "{source}" for stage "{stage}"')
            error = True

    if stage not in listeners:
        logging.error(f'action: checking | result: error | msg: no listeners for stage "{stage}"')
        error = True
    else:
        if notifier not in listeners[stage]:
            logging.error(f'action: checking | result: error | msg: notifier "{notifier}" is not listener of stage "{stage}"')
            error = True
        if source not in listeners[stage][notifier]:
            logging.error(f'action: checking | result: error | msg: no source "{source}" set for listener "{notifier}" in stage "{stage}"')
            error = True

    return error


def handler(ch, method, properties, body):

    msg = decode(body)
    logging.info(f'action: handle_msg | result: in_progress | msg: {msg}')

    splitted = msg.split(".")
    opcode, params = int(splitted[0]), splitted[1:]

    if EOF.is_reg(msg):

        stage, node = params[0], params[1]
        if stage not in listeners:
            listeners[stage] = {}

        if node not in listeners[stage]:
            listeners[stage][node] = {}

            for src in config[stage]["source"]:
                stc_name = src["name"]
                listeners[stage][node][stc_name] = {"EOF": False}
        else:
            logging.info(
                f'action: handle_register | result: warning | msg: node "{node}" already suscribed in stage "{stage}"')

        logging.info(f'action: handle_register | result: success | client: {node} | stage: {stage}')
        logging.debug(f'action: handle_register | result: success | stage: {stage} | listeners: {listeners[stage]}')

    elif EOF.is_eof(msg):

        stage, notifier, source = params
        original = properties.headers.get("original")
        logging.info(f'action: handle_eof | result: in_progress | stage: {stage} | node: {notifier} | source: {source}')

        if check_error(stage, notifier, source):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        listeners[stage][notifier][source]["EOF"] = True

        all_eof = True
        for node in listeners[stage]:
            for src in listeners[stage][node]:
                all_eof &= listeners[stage][node][src]["EOF"]

        if all_eof:
            publish_eof_to_next_stage(stage, rabbit)
        else:

            idx = list(map(lambda x: x["name"], config[stage]["source"])).index(source)
            if config[stage]["source"][idx]["type"] == "queue" and original == "true":
                notify_eof(stage, notifier, idx)

        logging.info(f'action: handle_eof | result: in_progress | stage_ready: {all_eof}')

    else:
        logging.info(f'action: handle_msg | result: fail | msg: no recognized opcode {opcode} with params {params}')

    logging.debug(f'action: handle_msg | result: done')
    ch.basic_ack(delivery_tag=method.delivery_tag)


def publish_eof_to_next_stage(stage_name, rabbit: RabbitInterface):

    for response_config in config[stage_name]["response"]:

        response_type = response_config["type"]
        output_name = response_config["name"]
        eof = EOF(stage_name, "manager", output_name).encode()

        if response_type == "topic":
            rabbit.publish_topic(output_name, eof, headers={"original": True})
            logging.info(f'action: publish_eof | result: in_progress | msg: published EOF in topic {output_name}')

        elif response_type == "queue":
            rabbit.publish_queue(output_name, eof, headers={"original": True})
            logging.info(f'action: publish_eof | result: in_progress | msg: published EOF in queue {output_name}')

        else:
            logging.info(f'action: publish_eof | result: fail | msg: unexpected response type {response_type}')
            return


def notify_eof(stage, notifier, source_idx):

    for node in listeners[stage]:
        logging.debug(f'action: pushing EOF | result: in_progress | stage: {stage} | node: {node}')
        if node == notifier: continue
        rabbit.publish_queue(config[stage]["source"][source_idx]["name"], EOF(stage, "manager").encode(), headers={"original": False})
        logging.debug(f'action: handle_eof | result: in_progress | msg: forwarding EOF to {node}')


# Status
config = load_config()
listeners = {}

rabbit = RabbitInterface()

logging.info(f'action: eof_manager | result: in_progress | msg: start listening from EOF_queue.')
rabbit.consume_queue("EOF_queue", handler)

logging.info(f'action: eof_manager | result: done')
