
WEATHER_FIELDS = 21
STATION_FIELDS = 6
TRIPS_FIELDS = 8

def handle_wheather(data):

    batch_size = int(data.pop(0))

    try:

        with open("weather.csv", "a") as file:
            for i in range(batch_size):
                reg = ",".join(data[i*WEATHER_FIELDS+1:(i+1)*WEATHER_FIELDS])
                file.write(reg+"\n")

    except:

        return False

    return True


def handle_stations(data):

    batch_size = int(data.pop(0))

    try:

        with open("stations.csv", "a") as file:
            for i in range(batch_size):
                reg = ",".join(data[i*STATION_FIELDS+1:(i+1)*STATION_FIELDS])
                file.write(reg+"\n")

    except:

        return False

    return True


def handle_trips(data):

    batch_size = int(data.pop(0))

    try:

        with open("trips.csv", "a") as file:
            for i in range(batch_size):
                reg = ",".join(data[i*TRIPS_FIELDS+1:(i+1)*TRIPS_FIELDS])
                file.write(reg+"\n")

    except:

        return False

    return True

