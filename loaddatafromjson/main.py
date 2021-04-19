import json
import os
from pymongo import MongoClient
import pandas as pd

def readjsonfromdir():
    paths = [
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterJanuar',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterFebruar',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterMaerz',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterApril',
    ]

    collections = [
        'ScooterJanuar',
        'ScooterFebruar',
        'ScooterMaerz',
        'ScooterApril',
    ]
    collCount = 0

    print('running main...')
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
    db = client['scooterdaten']
    #collection = db.scootercoll
    print('got client...')

    #path = r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterJanuar'

    for path in paths:
        os.chdir(path)
        collection = db[collections[collCount]]
        collCount= collCount + 1
        for file_name in [file for file in os.listdir(path) if file.endswith('.json')]:
          with open(path + '\\' + file_name) as json_file:
              print('reading file ' + file_name)
              for i, line in enumerate(json_file):
                    if i < 8:
                      continue
                    obj = json.loads(line)
                    try:
                        collection.insert_many(obj['data'])
                    except:
                        print('!!!a line could not be inserted!!!')
        client.close()
        print('finished reading from ' + path)
    print('finished reading data')



if __name__ == '__main__':
    readjsonfromdir()
    #for dbs in client.list_database_names():
     #  print(dbs)