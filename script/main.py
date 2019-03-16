import os
import sys
import json


def main():
    f = open(r'C:\Users\Naiyun\PycharmProjects\ProcessingTwitterCCC\data\tinyTwitter.json', encoding="utf8")
    data = json.load(f)

    f = open(r'C:\Users\Naiyun\PycharmProjects\ProcessingTwitterCCC\data\melbGrid.json', encoding="utf8")
    melbourne_grid = json.load(f)

    print(data[0]['geo']['coordinates'])
    print(data[1]['geo']['coordinates'])

    print(melbourne_grid["features"][])


if __name__ == '__main__':
    main()