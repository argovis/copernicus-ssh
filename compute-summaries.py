from pymongo import MongoClient
import datetime, json, copy, re
    
client = MongoClient('mongodb://database/argo')
db = client.argo

def get_timestamp_range(db, collection_name):
    collection = db['timeseriesMeta']
             
    filter = {"_id":collection_name}
    metadoc = collection.find_one(filter)
    earliest_timestamp = metadoc['timeseries'][0]
    latest_timestamp = metadoc['timeseries'][-1]

    # Convert timestamps to ISO 8601 format
    try:
        earliest_iso = earliest_timestamp.isoformat() + "Z"
        latest_iso = latest_timestamp.isoformat() + "Z"
        return earliest_iso, latest_iso
    except:
        return None, None

startDate, endDate = get_timestamp_range(db, 'copernicussla')
entry = {"metagroups": ["id"], "startDate": startDate, "endDate": endDate}

rldoc = db.summaries.find_one({"_id": 'ratelimiter'})
if rldoc:
    rldoc['metadata']['copernicussla'] = entry
else:
    rldoc = {"_id": "ratelimiter", "metadata": {"copernicussla": entry}}

try:        
    db.summaries.replace_one({"_id": 'ratelimiter'}, rldoc, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
