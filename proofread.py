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

#batchfiles = ["/bulk/copernicus-sla/sla_adt_mean_1993.nc","/bulk/copernicus-sla/sla_adt_mean_1994.nc","/bulk/copernicus-sla/sla_adt_mean_1995.nc","/bulk/copernicus-sla/sla_adt_mean_1996.nc","/bulk/copernicus-sla/sla_adt_mean_1997.nc","/bulk/copernicus-sla/sla_adt_mean_1998.nc","/bulk/copernicus-sla/sla_adt_mean_1999.nc","/bulk/copernicus-sla/sla_adt_mean_2000.nc","/bulk/copernicus-sla/sla_adt_mean_2001.nc","/bulk/copernicus-sla/sla_adt_mean_2002.nc","/bulk/copernicus-sla/sla_adt_mean_2003.nc","/bulk/copernicus-sla/sla_adt_mean_2004.nc","/bulk/copernicus-sla/sla_adt_mean_2005.nc","/bulk/copernicus-sla/sla_adt_mean_2006.nc","/bulk/copernicus-sla/sla_adt_mean_2007.nc","/bulk/copernicus-sla/sla_adt_mean_2008.nc","/bulk/copernicus-sla/sla_adt_mean_2009.nc","/bulk/copernicus-sla/sla_adt_mean_2010.nc","/bulk/copernicus-sla/sla_adt_mean_2011.nc","/bulk/copernicus-sla/sla_adt_mean_2012.nc","/bulk/copernicus-sla/sla_adt_mean_2013.nc","/bulk/copernicus-sla/sla_adt_mean_2014.nc","/bulk/copernicus-sla/sla_adt_mean_2015.nc","/bulk/copernicus-sla/sla_adt_mean_2016.nc","/bulk/copernicus-sla/sla_adt_mean_2017.nc","/bulk/copernicus-sla/sla_adt_mean_2018.nc","/bulk/copernicus-sla/sla_adt_mean_2019.nc","/bulk/copernicus-sla/sla_adt_mean_2020.nc","/bulk/copernicus-sla/sla_adt_mean_2021.nc","/bulk/copernicus-sla/sla_adt_mean_2022.nc"]
batchfiles = ["/bulk/copernicus-sla/sla_adt_mean_1993.nc"]

while True:
	lat = math.floor(random.random()*720) #math.floor(random.random()*720)
	lon = math.floor(random.random()*1440)

	# pick the corresponding mongo doc
	upstream = xarray.open_dataset(batchfiles[0], decode_times=False, mask_and_scale=False)
	latitude = upstream['latitude'][lat].to_dict()['data']
	longitude = tidylon(upstream['longitude'][lon].to_dict()['data'])
	_id = str(longitude) + "_" + str(latitude)
	doc = db.copernicusSLA.find_one({"_id": _id})

	if not doc:
		continue

	# reconstruct the list of means
	means = []
	for f in batchfiles:
		xar = xarray.open_dataset(f, decode_times=False, mask_and_scale=False)
		sla = xar.isel(latitude=lat, longitude=lon)['sla'].to_dict()['data']
		sla_nobs = xar.isel(latitude=lat, longitude=lon)['sla_nobs'].to_dict()['data']
		m = [sla[i] if nobs[i]==7 else -999.9 for i in range(len(sla))]
		means += m

	
	# compare with tolerance
	for i in range(len(means)):
		if (means[i] != -999.9 and round(means[i],4) != round(doc['data'][0][i],4)) or (means[i] == -999.9 and not math.isnan(doc['data'][0][i])):
			print("mismatch for profile " + _id + ' at ' + str(i))


	