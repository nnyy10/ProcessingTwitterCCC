from __future__ import print_function
import json
from pprint import pprint
import time
from mpi4py import MPI
import numpy as np
import re
import operator

# get start time
start_time = time.time()
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def construct_melb_grid(file_name):
    """Parse melbGrid.json file and put the value inside dictionary"""
    melb_grid = []

    with open(file_name) as f:
        data = json.load(f)
        for val in data["features"]:
            grid_data = {}
            properties = val["properties"]
            grid_data["id"] = properties["id"]
            grid_data["xmax"] = properties["xmax"]
            grid_data["xmin"] = properties["xmin"]
            grid_data["ymin"] = properties["ymin"]
            grid_data["ymax"] = properties["ymax"]
            grid_data["count"] = 0
            grid_data["hashtag_counts"] = {}
            grid_data["row_group"] = properties["id"][0:1]
            grid_data["column_group"] = properties["id"][1:2]
            melb_grid.append(grid_data)
    return melb_grid


def match_tweets_coordinates(melb_grid, lat, lng, hashtag_list):
    """Match individual tweet coordinates with the coordinates in melbGrid.json"""
    " lat -> y, long -> x "
    for grid_data in melb_grid:
        #todo needs to be changed, its double counting the ones that appear on the border
        if (lat >= grid_data["ymin"] and lat <= grid_data["ymax"]) \
                and (lng >= grid_data["xmin"] and lng <= grid_data["xmax"]):
            grid_data["count"] = grid_data["count"] + 1
            for hs in hashtag_list:
                if grid_data['hashtag_counts'].get(hs) is None:
                    grid_data["hashtag_counts"][hs] = 1
                else:
                    grid_data["hashtag_counts"][hs] = grid_data["hashtag_counts"][hs] + 1


MELB_GRID = construct_melb_grid('melbGrid.json')
FILE_NAME = 'smallTwitter.json'

# Sequential code for running on 1 core and 1 node (Don't need to split the big array)
if size < 2 and rank == 0:
    with open(FILE_NAME, encoding="utf8") as f:
        chunks = []
        # parse line by line in the file and ignore any error show up
        for line in f:
            try:
                processed_data = {}
                data = json.loads(line[0:len(line) - 2])
                processed_data["lat"] = data["value"]["geometry"]["coordinates"][0]
                processed_data["lng"] = data["value"]["geometry"]["coordinates"][1]
                text = data['value']['properties']['text']
                hashtag_list = re.findall(r"#(\w+)", text)
                processed_data["hashtags"] = hashtag_list
                print(processed_data)
                chunks.append(processed_data)
            except:
                pass

elif rank == 0: # Parallize routine
    with open(FILE_NAME, encoding="utf8") as f:
        coords_data = []
        # parse line by line in the file and ignore any error show up
        for line in f:
            try:
                processed_data = {}
                data = json.loads(line[0:len(line) - 2])
                processed_data["lat"] = data["value"]["geometry"]["coordinates"][0]
                processed_data["lng"] = data["value"]["geometry"]["coordinates"][1]
                text = data['value']['properties']['text']
                hashtag_list = re.findall(r"#(\w+)", text)
                processed_data["hashtags"] = hashtag_list
                print(processed_data)
                coords_data.append(processed_data)
            except:
                continue
    chunks = np.array_split(coords_data, size)
else:
    chunks = None


# check if it's running on 1 core 1 node, process the big array directly
# otherwise scatter the big array
if size < 2 and rank == 0:
    for data in chunks:
        match_tweets_coordinates(MELB_GRID, data["lng"], data["lat"], data["hashtags"])
    result = MELB_GRID
else:
    chunk = comm.scatter(chunks, root=0)

    for data in chunk:
        match_tweets_coordinates(MELB_GRID, data["lng"], data["lat"], data["hashtags"])

    # Gather all of results from child process
    result = comm.gather(MELB_GRID)

if rank == 0:
    ROW_RANK = {"A": 0, "B": 0, "C": 0, "D": 0}
    COLUMN_RANK = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    RESULT_GRID = {
        "A1": 0, "A2": 0, "A3": 0, "A4": 0,
        "B1": 0, "B2": 0, "B3": 0, "B4": 0,
        "C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0,
        "D3": 0, "D4": 0, "D5": 0,
    }

    if size < 2:
        for grid_data in result:
            RESULT_GRID[grid_data["id"]] = RESULT_GRID[grid_data["id"]] + grid_data["count"]
    else:
        for grid_data in result:
            for single_grid_data in grid_data:
                RESULT_GRID[single_grid_data["id"]] = RESULT_GRID[single_grid_data["id"]] + single_grid_data["count"]

    # Group by Row
    # Summarize the count by taking first character from the box name
    ROW_GROUP = {"A": 0, "B": 0, "C": 0, "D": 0}
    for i in RESULT_GRID:
        ROW_GROUP[i[0:1]] = ROW_GROUP[i[0:1]] + RESULT_GRID[i]

    # Group by column
    # Summarize the count by taking second character from the box name
    COLUMN_GROUP = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    for i in RESULT_GRID:
        COLUMN_GROUP[i[1:2]] = COLUMN_GROUP[i[1:2]] + RESULT_GRID[i]

    # Print all of the stuff here
    # Print rank by boxes
    print("\nRank based on boxes")
    GRID_RANKS = sorted(MELB_GRID, key=operator.itemgetter("count"), reverse=True)
    #GRID_RANKS = sorted(RESULT_GRID, key=RESULT_GRID.["count"], reverse=True)
    for i in GRID_RANKS:
        pprint('%s: %d tweets' % (i["id"], i["count"]))
        sorted_hs_list = sorted(i["hashtag_counts"].items(), key=operator.itemgetter(1), reverse=True)
        if 0 < sorted_hs_list.__len__() < 5:
            truncate_number = sorted_hs_list.__len__()
        else:
            truncate_number = 5
        if sorted_hs_list.__len__() > 0:
            print((i["id"], sorted_hs_list[:truncate_number]))

    # Print rank by rows
    print("\nOrder by rows")
    ROW_RANK = sorted(ROW_GROUP, key=ROW_GROUP.get, reverse=True)
    for val in ROW_RANK:
        pprint('%s-Row: %d' % (val, ROW_GROUP[val]))

    # Print rank by column
    print("\nOrder by columns")
    COLUMN_RANK = sorted(COLUMN_GROUP, key=COLUMN_GROUP.get, reverse=True)
    for val in COLUMN_RANK:
        pprint('Column %s: %d' % (val, COLUMN_GROUP[val]))

    # print the total time it takes
    total_minutes = time.time() - start_time
    minutes, seconds = divmod(total_minutes, 60)
    print("\nTotal time used for execution is %02d minutes and %02d seconds" %
          (minutes, seconds))