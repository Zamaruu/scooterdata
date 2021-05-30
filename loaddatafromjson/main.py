import json
import os
import pandas
from pymongo import MongoClient
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import geopy.distance

database = 'scooterdaten'
connection_string = 'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false'
collections = [
    'ScooterJanuar',
    'ScooterFebruar',
    'ScooterMaerz',
    'ScooterApril',
]

def readjsonfromdir():
    paths = [
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterJanuar',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterFebruar',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterMaerz',
        r'C:\Users\mditz\Desktop\ScooterDaten\ScooterDaten\ScooterApril',
    ]

    collCount = 0

    print('running main...')
    client = MongoClient(connection_string)
    db = client[database]

    print('got client...')

    for path in paths:
        os.chdir(path)
        db[collections[collCount]].drop()
        collection = db[collections[collCount]]
        collCount = collCount + 1
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

def loadDataFromDB():
    print('loading data')
    client = MongoClient(connection_string)
    db = client[database]
    collection = db[collections[0]]

    test_subject = collection.find_one()
    id = test_subject['id']
    print('Id of the first element in the collection: ' + id)

    x = collection.find({}, {
        'id': id,
        'batteryLevel': 1,
        'lastStateChange': 1
    })

    avgpipe = [
        {"$match":
            {"id": "ddbb29fd-bf4f-4cb5-b7a1-1a35489a1832"}
        },
        {"$group": {
                "_id": "$id",
                "batavg": {"$avg": "$batteryLevel"}
            },
        },
    ]

    avgres = collection.aggregate(pipeline=avgpipe)

    for each in avgres:
        print("Avarage batteryLevel of Scooter '"+each['_id']+"' is: " + "%.2f" % each['batavg'] + "%")

    latlngpipe = [
        {"$match":
             {"id": "ddbb29fd-bf4f-4cb5-b7a1-1a35489a1832"}
         },
        {"$group": {
            "_id": "$id",
            "lastLocationUpdate": "$lastLocationUpdate",
            "lat": "$lat",
            "lng": "$lng",
            },
        },
    ]
    query = {
        'id': id,
        "lastLocationUpdate": 1,
        "lat": 1,
        "lng": 1,
    }
    clear = lambda: os.system('cls')
    posres = collection.find({}, query)
    print("posres: " + str(type(posres)))
    print(posres)
    distances = []
    positions = []
    cnt = 0
    #posres = collection.aggregate(pipeline=latlngpipe)
    for point in posres:
        cnt += 1
        print("loading coordinates #" + str(cnt))
        #print("Point '" + str(point['lat']) + ", " + str(point['lng']) + " on " + point['lastLocationUpdate'])
        positions.append(
            {
                'lat': point['lat'],
                'lng': point['lng'],
                'lastLocationUpdate': point['lastLocationUpdate'],
            }
        )
        #coords_1 = (point['lat'], point['lng'])
        #coords_2 = (point[i + 1]['lat'], point[i + 1]['lng'])
        #distances.append(geopy.distance.vincenty(coords_1, coords_2).km)
        #i = i + 1
        #try:

        #except:
        #    print("EOL")
    print("finsished collecting data\nProccessing Data now!")
    i = 0
    for point in positions:
        coords_1 = (point['lat'], point['lng'])
        coords_2 = (positions[i + 1]['lat'], positions[i + 1]['lng'])
        distances.append(geopy.distance.distance(coords_1, coords_2).km)
    avgdist = sum(distances) / len(distances)
    print("Avarage distancetravel with scooter " + id + ": "+ str(int(avgdist)) + "km")

    # die ersten 10 ergebnisse ausgeben
    #df = pandas.DataFrame(list(x[:25]))
    #df.plot(kind="bar", x="lastStateChange", y="batteryLevel")
    #plt.show()


    avarageBatteryLevel = 0
    #for data in x:
    #    avarageBatteryLevel += int(x['batteryLevel'])


    #avarageBatteryLevel = avarageBatteryLevel / list(x).
    #print('Durchschnittliche Batterieladung des Scooters: ' + str(avarageBatteryLevel) + '%')

    #engine = create_engine("mongodb:///?Server=MyServer&;Port=27017&Database=ScooterDaten&User=test&Password=Password")
    #df = pandas.read_sql("SELECT borough, cuisine FROM restaurants WHERE Name = 'Morris Park Bake Shop'", engine)
    #df.plot(kind="bar", x="borough", y="cuisine")
    #plt.show()

if __name__ == '__main__':
    loadDataFromDB()
    #readjsonfromdir()
