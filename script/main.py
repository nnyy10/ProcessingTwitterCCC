import json
import numpy
import math
import re
import time
from mpi4py import MPI
import operator

# get start time
start_time = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def main():
    path = "smallTwitter.json"
    f = open(path, encoding="utf8")
    data = json.load(f)["rows"]

    path = "melbGrid.json"
    f = open(path, encoding="utf8")
    melbourne_grid = json.load(f)

    areas = []

    max_column = 0
    max_row = 0
    for a in melbourne_grid["features"]:
        area = a["properties"]
        area_object = Area(area["id"], area["xmin"], area["xmax"], area["ymin"], area["ymax"])
        areas.append(area_object)
        #print(area_object.name, area_object.x_min, area_object.x_max, area_object.y_min, area_object.y_max)
        if int(list(area["id"])[1]) - 1 > max_column:
            max_column = int(list(area["id"])[1]) - 1
        if ord(list(area["id"])[0]) - 65 > max_row:
            max_row = ord(list(area["id"])[0]) - 65

    area_matrix = numpy.empty([max_row + 1, max_column+1], dtype=Area)

    for a in areas:
        ax = int(list(a.name)[1]) - 1
        ay = ord(list(a.name)[0]) - 65
        area_matrix[ay, ax] = a

    for d in data:
        # if d['value']['geometry'] is None:
        #     continue

        x = math.floor((d['value']['geometry']['coordinates'][0] - 144.7) / 0.15)
        y = math.floor(((d['value']['geometry']['coordinates'][1]+37.5)*(-1))/0.15)
        if 0 <= x < max_row and 0 <= y < max_column and area_matrix[y, x] is not None:
            print(d['value']['geometry']['coordinates'][0])
            print(d['value']['geometry']['coordinates'][1])
            print(area_matrix[y, x].name)
            area_matrix[y, x].tweet_number += 1

            hashtags = get_hashtags(d['value']['properties']['text'])
            hashtag_dictionary = area_matrix[y, x].twitter_hashtags
            for hs in hashtags:
                hashtag_number = hashtag_dictionary.get(hs)
                if hashtag_number is None:
                    hashtag_dictionary[hs] = 1
                else:
                    hashtag_dictionary[hs] = hashtag_number + 1

    for l in area_matrix:
        for o in l:
            if o is not None:
                print(o.name, ": ", o.tweet_number)
                print(o.name, ": ", sorted(o.twitter_hashtags.items(), key=operator.itemgetter(1), reverse=True)[:5])
                print(o.name, ": ", o.twitter_hashtags)
    
    found = False
    for a in data:
        if 144.700000 < a['value']['geometry']['coordinates'][0] < 144.850000 and -37.650000 < a['value']['geometry']['coordinates'][1] < -37.500000:
            found = True
            
    print(found)

def get_hashtags(twitter_text):
    return re.findall(r"#(\w+)", twitter_text)

class Area:
    tweet_number = 0

    def __init__(self, name, x_min, x_max, y_min, y_max):
        self.name = name
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max
        self.twitter_hashtags = {}

if __name__ == '__main__':
    main()
    twitter_hashtags = {"friend": 1}
    print(twitter_hashtags.get("d"))
    twitter_hashtags["d"] = 1
    print(twitter_hashtags)
