import json
import os
import pandas as pd
from pymongo import MongoClient
from operator import itemgetter
import matplotlib.pyplot as plt
import geopy.distance
from datetime import datetime

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

def dbconnect(targetcoll):
    client = MongoClient(connection_string)
    db = client[database]
    collection = db[targetcoll]
    return collection

def getUniqueIDs(collection):
    collection = dbconnect(collection)
    res = list(collection.distinct("id"))
    print(str(len(res)) + " unique IDs")
    return res

def getUniqueIDsWithPipeline(collection, query):
    collection = dbconnect(collection)
    res = list(collection.find(
        {
            "id": 1,
            "licencePlate": "",
        }
    ).distinct("id"))
    print(res)
    #print(str(len(res)) + " unique IDs")
    return res

def plotAverageBatteryLevel(collection):
    ids = getUniqueIDs(collection)
    avglevels = []

    print('Collecting data from every Scooter (this may take a while)')

    i = 1
    for id in ids[:5]:
        print(i)
        avglevels.append(
            {
                "id": id,
                "avg": getAverageBatteryLevel(id)
            }
        )
        i += 1

    sorted(avglevels, key=lambda k: k['avg'])
    print(type(avglevels[0]), avglevels[0])
    avgbattlevel = sum(scooter['avg'] for scooter in avglevels) / len(avglevels)
    print("Avarage batteryLevel of Scooter in Month'" + collection + "' is: " + "%.2f" % avgbattlevel + "%")


    df = pd.DataFrame(avglevels)
    df.sort_values(by=['avg'])
    plt.bar(df['id'], df['avg'])
    plt.title("Avarage batteryLevel of Scooter in Month'" + collection + "' is: " + "%.2f" % avgbattlevel + "%", fontsize=10)
    plt.axhline(y=avgbattlevel, linewidth=1, color='k')
    plt.ylabel('Avg Battery Level')
    plt.xlabel('Scooter ID')
    plt.xticks(rotation=90)
    plt.show()

def getAverageBatteryLevel(id):
    collection = dbconnect(collections[0])

    avgpipe = [
        {"$match":
            {"id": id}
        },
        {"$group": {
                "_id": "$id",
                "batavg": {"$avg": "$batteryLevel"}
            },
        },
    ]

    avgres = collection.aggregate(pipeline=avgpipe)

    for each in avgres:
        #print("Avarage batteryLevel of Scooter '"+each['_id']+"' is: " + "%.2f" % each['batavg'] + "%")
        return each['batavg']

def plotAverageTraveldistance(id):
    collection = dbconnect(collections[0])

    query = {
        'id': id,
        "lastStateChange": 1,
        "lat": 1,
        "lng": 1,
    }

    posres = collection.find({}, query)
    print("posres: " + str(type(posres)))
    print(posres)
    distances = []
    positions = []
    cnt = 0

    for point in posres[:200]:
        cnt += 1
        print("loading coordinates #" + str(cnt))

        positions.append(
            {
                'lat': point['lat'],
                'lng': point['lng'],
                'lastStateChange': point['lastStateChange'],
            }
        )

    print("finsished collecting data\nProccessing Data now!")
    i = 0
    for point in positions:
        coords_1 = (point['lat'], point['lng'])
        coords_2 = (positions[i + 1]['lat'], positions[i + 1]['lng'])
        date = datetime.fromisoformat(point['lastStateChange'][:-1])
        distances.append({
            #"date": date.strftime('%d.%m.%Y'),
            "date": point['lastStateChange'],
            "distance": geopy.distance.distance(coords_1, coords_2).km
        })
    print(len(distances))
    #print(distances)
    avgdist = sum(scooter['distance'] for scooter in distances) / len(distances)
    print("Avarage distancetravel with scooter " + id + ": " + "%.1f" % avgdist + "km")

    df = pd.DataFrame(distances)
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['date'], ascending=False)
    #df['date'] = df['date'].dt.strftime('%d-%m-%Y')
    plt.bar(df['date'], df['distance'], width=0.05)
    plt.title("Avarage distancetravel with scooter " + id + ": " + "%.1f" % avgdist + "km", fontsize=10)
    plt.axhline(y=avgdist, linewidth=1, color='red')
    plt.ylabel('Avg distance traveled')
    plt.xlabel('Dates')
    plt.xticks(rotation=45)
    plt.show()

def plotScooterUsageMap(id):
    collection = dbconnect(collections[0])

    query = {
        'id': id,
        "lastStateChange": 1,
        "lat": 1,
        "lng": 1,
    }
    posres = collection.find({}, query)
    print("posres: " + str(type(posres)))
    print(posres)

    positions = []
    cnt = 0
    for point in posres[:10]:
        cnt += 1
        print("loading coordinates #" + str(cnt))

        positions.append(
            {
                'lat': point['lat'],
                'lng': point['lng'],
                'date': point['lastStateChange'],
            }
        )

    print(positions)
    positions.sort(key=itemgetter('date'), reverse=False)

    print(positions)
    poslistlen = len(positions)
    df = pd.DataFrame(positions)

    bbox = [8.7000, 8.8133, 51.7438, 51.6952]

    os.chdir(r'C:\Users\mditz\Desktop\ScooterDaten\PBMap')
    pb_map = plt.imread('map.png')

    fig, ax = plt.subplots(figsize = (10,7))
    ax.scatter(df['lng'], df['lat'],  zorder=1, c='r', s=10)

    cnt = 0
    for position in positions:
        if cnt == 0:
            print("start")
        elif cnt == poslistlen-1:
            print("end")
        else:
            ax.annotate(position['date'], xy=(position['lng'], position['lat']),
                        xytext=(-20, 20),
                        textcoords='offset points', ha='center', va='bottom',
                        bbox=dict(boxstyle='round,pad=0.2', fc='yellow', alpha=0.3),
                        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.5',
                                        color='red'))
        cnt += 1

    ax.annotate('Startpunkt am \n' + positions[0]['date'] , xy=(positions[0]['lng'], positions[0]['lat']), xytext=(20, 20),
                textcoords='offset points', ha='center', va='bottom',
                bbox=dict(boxstyle='round,pad=0.2', fc='blue', alpha=0.3),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.5',
                                color='red'))
    ax.annotate('Endpunkt  am \n' + positions[poslistlen-1]['date'], xy=(positions[poslistlen-1]['lng'], positions[poslistlen-1]['lat']), xytext=(20, 20),
                textcoords='offset points', ha='center', va='bottom',
                bbox=dict(boxstyle='round,pad=0.2', fc='blue', alpha=0.3),
                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.5',
                                color='red'))
    ax.set_title('Plotting Spatial Scooter Data on Paderborn Map / SID: ' + id)
    #plt.title('Title')
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.imshow(pb_map, zorder=0, extent = bbox,)
    plt.plot(df['lng'], df['lat'])
    plt.show()

if __name__ == '__main__':
    collection = dbconnect(collections[0])

    test_subject = collection.find_one()
    id = test_subject['id']

    #plotAverageBatteryLevel(collections[0])
    #input("Press Enter to continue...")
    plotAverageTraveldistance(id)
    plotScooterUsageMap(id)
    #readjsonfromdir()
