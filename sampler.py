from random import random

DATA_PATH="origen"
destino = "destino"

cities = ["toronto", "montreal", "washington"]
trip_file="trips.csv"

PROB=0.6

with open(f"{DATA_PATH}\\montreal\\{trip_file}") as original:
    with open(f"{destino}\\montreal\\trip-sampled.csv", "w") as sampled:
        line = original.readline()
        while line:

            if '2016' <= line[:4] <= '2017':
                if random()<PROB:
                     sampled.write(line)

                # sampled.write(line)

            line = original.readline()


