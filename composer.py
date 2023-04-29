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
    "volumes": ["./server/:/app/"]
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
        client_aux["environment"].append(f"WEATHER_FILE=/app/data/weather.csv")
        client_aux["environment"].append(f"STATIONS_FILE=/app/data/stations.csv")
        client_aux["environment"].append(f"TRIPS_FILE=/app/data/trips_test.csv")
        services[service_name] = client_aux

    services["weather_filter"] = deepcopy(filter)
    services["weather_filter"]["container_name"] = "weather_filter"
    services["weather_filter"]["entrypoint"] = "python3 /app/weather_filter.py"
    services["weather_filter"]["volumes"] = ["./filters/weather/:/app/"]

    services["station_filter"] = deepcopy(filter)
    services["station_filter"]["container_name"] = "station_filter"
    services["station_filter"]["entrypoint"] = "python3 /app/station_filter.py"
    services["station_filter"]["volumes"] = ["./filters/station/:/app/"]

    services["trip_filter"] = deepcopy(filter)
    services["trip_filter"]["container_name"] = "trip_filter"
    services["trip_filter"]["entrypoint"] = "python3 /app/trip_filter.py"
    services["trip_filter"]["volumes"] = ["./filters/trip/:/app/"]

    services["trip_weather_joiner"] = deepcopy(filter)
    services["trip_weather_joiner"]["container_name"] = "trip_weather_joiner"
    services["trip_weather_joiner"]["entrypoint"] = "python3 /app/trip_weather_joiner.py"
    services["trip_weather_joiner"]["volumes"] = ["./joiners/trip_weather/:/app/"]

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
