import os
import json
import numpy
import math


def main():

    path = os.path.join("..", "data", "tinyTwitter.json")
    f = open(path, encoding="utf8")
    data = json.load(f)

    path = os.path.join("..", "data", "melbGrid.json")
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

    area_matrix = numpy.empty([max_row + 1, max_column+1],dtype=Area)

    for a in areas:
        ax = int(list(a.name)[1]) - 1
        ay = ord(list(a.name)[0]) - 65
        area_matrix[ay, ax] = a

    for i in range(0, 1000):
        if data[i]['geo'] is None:
            continue
        print(data[i]['geo']['coordinates'][1])
        print(data[i]['geo']['coordinates'][0])
        x = math.floor((data[i]['geo']['coordinates'][1] - 144.7) / 0.15)
        y = math.floor(((data[i]['geo']['coordinates'][0]+37.5)*(-1))/0.15)
        if x>=0 and y>=0 and x < max_row and y < max_column:
            if area_matrix[y,x] is not None:
                print(area_matrix[y,x].name)
                area_matrix[y, x].tweet_number += 1
        print("\n")

    for l in area_matrix:
        for o in l:
            if o is not None:
                print(o.name, ": ", o.tweet_number)


class Area:
    tweet_number = 0

    def __init__(self, name, x_min, x_max, y_min, y_max):
        self.name = name
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max


if __name__ == '__main__':
    main()
