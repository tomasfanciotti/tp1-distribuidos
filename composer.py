from copy import copy, deepcopy
from sys import argv
import yaml

MAX_CLIENTS = 10
DEFAULT_CLIENTS = 1

PROJECT_NAME = "tp1"
NETWORK_NAME = "analyzer_net"

version = '3.9'

# Templates

server = {
    "container_name": "server",
    "image": "server:latest",
    "entrypoint": "python3 /app/main.py",
    "environment": ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=DEBUG"],
    "networks": [NETWORK_NAME],
    "volumes": ["./server/:/app/"],
    "restart": "on-failure"
}

network = {
    "ipam": {
        "driver": "default",
        "config": [
            {"subnet": "172.25.125.0/24"}
        ]
    }
}

client = {
    "container_name": "client",
    "image": "client:latest",
    "entrypoint": "/app/client",
    "environment": ["LOGGING_LEVEL=DEBUG"],
    "networks": [NETWORK_NAME],
    "depends_on": ["server"],
    "volumes": ["./client/:/app/config/", "./.data/dataset/:/app/data/", ]
}

rabbit = {
    "container_name": "rabbitmq",
    "image": "rabbitmq:latest",
    "networks": [NETWORK_NAME],
    "ports" : ["15672:15672"],
    "healthcheck" : {
        "test" : '["CMD", "curl", "-f", "http://localhost:15672"]',
        "interval": "10s",
        "timeout": "5s",
        "retries": 10
    }
}


filter = {
    "image": "python-filter:latest",
    "networks": [NETWORK_NAME],
    "depends_on": ["rabbitmq", "server"],
    "restart": "on-failure",
    "environment": ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=DEBUG"],
    "volumes": ["./server/common/messaging_protocol.py:/app/messaging_protocol.py",
                "./server/common/rabbit_interface.py:/app/rabbit_interface.py",
                "./server/common/eof.py:/app/eof.py"
                ]
}

eof_manager = {
    "container_name": "eof_manager",
    "image": "python-filter:latest",
    "networks": [NETWORK_NAME],
    "entrypoint": "python3 /app/eof_manager.py",
    "depends_on": ["rabbitmq", "server"],
    "environment": ["PYTHONUNBUFFERED=1", "LOGGING_LEVEL=DEBUG"],
    "volumes":  ["./server/common/:/app/"]
}


def generate(clients):
    config = {}
    services = {"server": server}

    clients = min(clients, MAX_CLIENTS)
    for i in range(clients):
        service_name = "client-" + str(i + 1)
        client_aux = deepcopy(client)
        client_aux["container_name"] = service_name
        client_aux["environment"].append(f"CLI_ID={i + 1}")
        client_aux["environment"].append(f"DATA_SOURCE=/app/data/")
        services[service_name] = client_aux

    # Raw Data Filters
    services["weather_filter"] = deepcopy(filter)
    services["weather_filter"]["container_name"] = "weather_filter"
    services["weather_filter"]["entrypoint"] = "python3 /app/weather_filter.py"
    services["weather_filter"]["volumes"].append("./filters/weather/weather_filter.py:/app/weather_filter.py")

    services["station_filter"] = deepcopy(filter)
    services["station_filter"]["container_name"] = "station_filter"
    services["station_filter"]["entrypoint"] = "python3 /app/station_filter.py"
    services["station_filter"]["volumes"].append("./filters/station/station_filter.py:/app/station_filter.py")

    services["trip_filter"] = deepcopy(filter)
    services["trip_filter"]["container_name"] = "trip_filter"
    services["trip_filter"]["entrypoint"] = "python3 /app/trip_filter.py"
    services["trip_filter"]["volumes"].append("./filters/trip/trip_filter.py:/app/trip_filter.py")

    # Joiners
    services["trip_weather_joiner"] = deepcopy(filter)
    services["trip_weather_joiner"]["container_name"] = "trip_weather_joiner"
    services["trip_weather_joiner"]["entrypoint"] = "python3 /app/trip_weather_joiner.py"
    services["trip_weather_joiner"]["volumes"].append("./joiners/trip_weather/trip_weather_joiner.py:/app/trip_weather_joiner.py")

    services["trip_station_joiner"] = deepcopy(filter)
    services["trip_station_joiner"]["container_name"] = "trip_station_joiner"
    services["trip_station_joiner"]["entrypoint"] = "python3 /app/trip_station_joiner.py"
    services["trip_station_joiner"]["volumes"].append("./joiners/trip_station/trip_station_joiner.py:/app/trip_station_joiner.py")

    # Query 1
    services["query1_filter1"] = deepcopy(filter)
    services["query1_filter1"]["container_name"] = "query1_filter"
    services["query1_filter1"]["entrypoint"] = "python3 /app/prectot_filter.py"
    services["query1_filter1"]["volumes"].append("./filters/query1/prectot_filter.py:/app/prectot_filter.py")

    services["query1_filter2"] = deepcopy(filter)
    services["query1_filter2"]["container_name"] = "query1_average_calc"
    services["query1_filter2"]["entrypoint"] = "python3 /app/average_calc.py"
    services["query1_filter2"]["volumes"].append("./filters/query1/average_calc.py:/app/average_calc.py")

    # Query 2
    services["query2_filter1"] = deepcopy(filter)
    services["query2_filter1"]["container_name"] = "filter_by_trips"
    services["query2_filter1"]["entrypoint"] = "python3 /app/filter.py"
    services["query2_filter1"]["volumes"].append("./filters/query2/filter.py:/app/filter.py")

    services["query2_filter2"] = deepcopy(filter)
    services["query2_filter2"]["container_name"] = "counter"
    services["query2_filter2"]["entrypoint"] = "python3 /app/counter.py"
    services["query2_filter2"]["volumes"].append("./filters/query2/counter.py:/app/counter.py")

    # Query 3
    services["query3_filter1"] = deepcopy(filter)
    services["query3_filter1"]["container_name"] = "join_stations"
    services["query3_filter1"]["entrypoint"] = "python3 /app/join_stations.py"
    services["query3_filter1"]["volumes"].append("./filters/query3/join_stations.py:/app/join_stations.py")

    services["query3_filter2"] = deepcopy(filter)
    services["query3_filter2"]["container_name"] = "distance_calc"
    services["query3_filter2"]["entrypoint"] = "python3 /app/distance_calc.py"
    services["query3_filter2"]["volumes"].append("./filters/query3/distance_calc.py:/app/distance_calc.py")

    services["query3_filter3"] = deepcopy(filter)
    services["query3_filter3"]["container_name"] = "average_calc"
    services["query3_filter3"]["entrypoint"] = "python3 /app/average_calc.py"
    services["query3_filter3"]["volumes"].append("./filters/query3/average_calc.py:/app/average_calc.py")

    services["query3_filter4"] = deepcopy(filter)
    services["query3_filter4"]["container_name"] = "filter_by_avg"
    services["query3_filter4"]["entrypoint"] = "python3 /app/filter.py"
    services["query3_filter4"]["volumes"].append("./filters/query3/filter.py:/app/filter.py")

    services["eof_manager"] = eof_manager
    services["rabbitmq"] = rabbit

    config["services"] = services
    config["version"] = version
    config["networks"] = {NETWORK_NAME : network}
    config["name"] = PROJECT_NAME

    return config


def main():
    print(argv)
    if len(argv) == 3 and argv[1] == "--clients" and argv[1].isdigit():
        clients = int(argv[2])
    else:
        clients = DEFAULT_CLIENTS

    config = generate(clients)
    print("Servicios configurados: ", len(config["services"]))

    with open("docker-compose-gen.yaml", "w") as docc_file:
        yaml.dump(config, docc_file)

    with open("server/network", "w") as net_file:
        net_file.write(PROJECT_NAME + "_" + NETWORK_NAME)


if __name__ == "__main__":
    main()
