import datetime, xarray, random, math
from pymongo import MongoClient

def tidylon(longitude):
    # map longitude on [0,360] to [-180,180], required for mongo indexing
    if longitude <= 180.0:
        return longitude;
    else:
        return longitude-360.0;

# db connection
client = MongoClient('mongodb://database/argo')
db = client.argo

batchfiles = ["data/ssh_mean_9394.nc","data/ssh_mean_9596.nc","data/ssh_mean_9798.nc","data/ssh_mean_9900.nc","data/ssh_mean_0102.nc","data/ssh_mean_0304.nc","data/ssh_mean_0506.nc","data/ssh_mean_0708.nc","data/ssh_mean_0910.nc","data/ssh_mean_1112.nc","data/ssh_mean_1314.nc","data/ssh_mean_1516.nc","data/ssh_mean_1718.nc","data/ssh_mean_1920.nc","data/ssh_mean_2122.nc"]

while True:
	lat = 175 # math.floor(random.random()*720)
	lon = math.floor(random.random()*1440)

	# pick the corresponding mongo doc
	upstream = xarray.open_dataset(batchfiles[0], decode_times=False, mask_and_scale=False)
	latitude = upstream['latitude'][lat].to_dict()['data']
	longitude = tidylon(upstream['longitude'][lon].to_dict()['data'])
	_id = str(longitude) + "_" + str(latitude)
	doc = db.copernicusSLA.find_one({"_id": _id})

	# reconstruct the list of means
	means = []
	for f in batchfiles:
		xar = xarray.open_dataset(f, decode_times=False, mask_and_scale=False)
		sla = xar.isel(latitude=lat, longitude=lon)['sla'].to_dict()['data']
		nobs = xar.isel(latitude=lat, longitude=lon)['nobs'].to_dict()['data']
		m = [sla[i] if nobs[i]==7 else -999.9 for i in range(len(sla))]
		means += m

	
	# compare with tolerance
	for i in range(len(means)):
		if (means[i] != -999.9 and round(means[i],4) != round(doc['data'][0][i],4)) or (means[i] == -999.9 and not math.isnan(doc['data'][0][i])):
			print("mismatch for profile " + _id + ' at ' + str(i))


	