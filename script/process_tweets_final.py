from mpi4py import MPI
import numpy as np
import json
import math
import operator
import time

twitter_file_name = "smallTwitter.json"

# get start time
start_time = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
amode = MPI.MODE_RDONLY

def construct_melb_grid():
    path = "melbGrid.json"
    with open(path, encoding="utf8") as f:
        melbourne_grid = json.load(f)
        areas = []
        max_column = 0
        max_row = 0
        for val in melbourne_grid["features"]:
            grid_data = {}
            properties = val["properties"]
            grid_data["id"] = properties["id"]
            grid_data["count"] = 0
            grid_data["hashtag_counts"] = {}
            areas.append(grid_data)
            if int(list(grid_data["id"])[1]) - 1 > max_column:
                max_column = int(list(grid_data["id"])[1]) - 1
            if ord(list(grid_data["id"])[0]) - 65 > max_row:
                max_row = ord(list(grid_data["id"])[0]) - 65

        area_matrix = np.empty([max_row + 1, max_column + 1], dict)

        for grid_data in areas:
            ax = int(list(grid_data["id"])[1]) - 1
            ay = ord(list(grid_data["id"])[0]) - 65
            area_matrix[ay, ax] = grid_data
        return area_matrix

area_matrix = construct_melb_grid()
max_row = np.size(area_matrix, 0)
max_column = np.size(area_matrix, 1)

def find_hashtags(text):
    hs_list = []
    for i in range(0,text.__len__()):
        if text[i] == '#' and i != 0 and i+1 != text.__len__() and text[i+1] != ' ':
            j = i + 1
            while j < text.__len__() and text[j] != ' ':
                j += 1
            if j!=text.__len__():
                hs_list.append(text[i:j].lower())
    return hs_list

def process_data(data):
    cnt = 0
    for line in data:
        if rank != cnt % size:
            cnt += 1
            continue
        cnt += 1
        try:
            line_json = json.loads(line[0:len(line) - 2])
        except:
            try:
                line_json = json.loads(line[0:len(line) - 1])
            except:
                continue
        try:
            x_raw = line_json['doc']['coordinates']['coordinates'][0]
            y_raw = line_json['doc']['coordinates']['coordinates'][1]
        except:
            try:
                x_raw = line_json['doc']['geo']['coordinates'][1]
                y_raw = line_json['doc']['geo']['coordinates'][0]
            except:
                try:
                    x_raw = line_json['value']['geometry']['coordinates'][0]
                    y_raw = line_json['value']['geometry']['coordinates'][1]
                except:
                    continue

        x = math.floor((x_raw - 144.7) / 0.15)
        if not 0 <= x < max_column: continue
        y = math.floor(((y_raw + 37.5) * (-1)) / 0.15)
        if not 0 <= y < max_row: continue

        if area_matrix[y, x] is not None:
            area_matrix[y, x]["count"] += 1
            try:
                hashtags = find_hashtags(line_json['doc']['text'])
            except:
                continue
            hashtag_dictionary = area_matrix[y, x]["hashtag_counts"]
            for hs in hashtags:
                hashtag_number = hashtag_dictionary.get(hs)
                if hashtag_number is None:
                    hashtag_dictionary[hs] = 1
                else:
                    hashtag_dictionary[hs] = hashtag_number + 1

area_matrix = construct_melb_grid()
max_row = np.size(area_matrix, 0)
max_column = np.size(area_matrix, 1)

with open(twitter_file_name, encoding="utf8") as data:
    process_data(data)

if size > 1:
    area_matrix_array = comm.gather(area_matrix)
if rank == 0 and size > 1:
    area_matrix = construct_melb_grid()
    for am in area_matrix_array:
        for i in range(0, max_column):
            for j in range(0, max_row):
                if area_matrix[j,i] == None:
                    continue
                area_matrix[j][i]["count"] = area_matrix[j][i]["count"] + am[j][i]["count"]
                for hashtag in am[j][i]["hashtag_counts"]:
                    hashtag_count = area_matrix[j][i]["hashtag_counts"].get(hashtag)
                    if hashtag_count is None:
                        area_matrix[j][i]["hashtag_counts"][hashtag] = am[j][i]["hashtag_counts"][hashtag]
                    else:
                        area_matrix[j][i]["hashtag_counts"][hashtag] = hashtag_count + am[j][i]["hashtag_counts"][hashtag]

if rank == 0:
    area_list = area_matrix.tolist()
    area_list = area_list[0] + area_list[1] + area_list[2] + area_list[3]
    area_list = [i for i in area_list if i]
    area_list = sorted(area_list, key=operator.itemgetter("count"), reverse=True)
    for area in area_list:
        if area is not None:
            print(area["id"], ": ", area["count"])
            sorted_hashtags = sorted(area["hashtag_counts"].items(), key=operator.itemgetter(1), reverse=True)

            cnt = 0
            pre = None
            threshold = 5
            for i in range(0, sorted_hashtags.__len__()):
                if cnt == 6:
                    break
                if sorted_hashtags[i][1] != pre:
                    cnt += 1
                else:
                    threshold += 1
                pre = sorted_hashtags[i][1]
            print(sorted_hashtags[0:threshold])

    total_minutes = time.time() - start_time
    minutes, seconds = divmod(total_minutes, 60)
    print("\nTotal time used for execution is %02d minutes and %02d seconds" %
          (minutes, seconds))
    MPI.Finalize()
