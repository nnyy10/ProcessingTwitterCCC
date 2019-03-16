import os
import sys
import json


def main():
    print("lol")
    f = open(r'C:\Users\Naiyun\PycharmProjects\ProcessingTwitterCCC\data\reallyreallysmallTwitter.json')
    data = json.load(f)

    print(data[0]['geo']['coordinates'])
    print(data[1]['geo']['coordinates'])

if __name__ == '__main__':
    main()