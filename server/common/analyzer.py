
WEATHER_FIELDS = 21


def handle_wheather(data):

    batch_size = int(data.pop(0))

    try:
        with open("output.csv", "w") as file:
            for i in range(batch_size):
                reg = ",".join(data[i*WEATHER_FIELDS+1:(i+1)*WEATHER_FIELDS])
                file.write(reg+"\n")
    except:

        return False

    return True