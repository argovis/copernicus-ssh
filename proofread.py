import datetime, xarray, random, math

def timewindow(start, duration):
    # given a string specifying the central date in the format "1993-02-07T00:00:00.000Z",
    # produce a list of strings for the days +- radius around that date in the format yyyymmdd

    centerdatetime = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
    datestrings = []
    for i in range(duration):
    	datestrings.append( (centerdatetime + datetime.timedelta(days=i)).strftime("%Y%m%d") )

    return datestrings

while True:
	timelattice = ["1993-01-03T00:00:00.000Z","1993-01-10T00:00:00.000Z","1993-01-17T00:00:00.000Z","1993-01-24T00:00:00.000Z","1993-01-31T00:00:00.000Z","1993-02-07T00:00:00.000Z","1993-02-14T00:00:00.000Z","1993-02-21T00:00:00.000Z","1993-02-28T00:00:00.000Z","1993-03-07T00:00:00.000Z","1993-03-14T00:00:00.000Z","1993-03-21T00:00:00.000Z","1993-03-28T00:00:00.000Z","1993-04-04T00:00:00.000Z","1993-04-11T00:00:00.000Z","1993-04-18T00:00:00.000Z","1993-04-25T00:00:00.000Z","1993-05-02T00:00:00.000Z","1993-05-09T00:00:00.000Z","1993-05-16T00:00:00.000Z","1993-05-23T00:00:00.000Z","1993-05-30T00:00:00.000Z","1993-06-06T00:00:00.000Z","1993-06-13T00:00:00.000Z","1993-06-20T00:00:00.000Z","1993-06-27T00:00:00.000Z","1993-07-04T00:00:00.000Z","1993-07-11T00:00:00.000Z","1993-07-18T00:00:00.000Z","1993-07-25T00:00:00.000Z","1993-08-01T00:00:00.000Z","1993-08-08T00:00:00.000Z","1993-08-15T00:00:00.000Z","1993-08-22T00:00:00.000Z","1993-08-29T00:00:00.000Z","1993-09-05T00:00:00.000Z","1993-09-12T00:00:00.000Z","1993-09-19T00:00:00.000Z","1993-09-26T00:00:00.000Z","1993-10-03T00:00:00.000Z","1993-10-10T00:00:00.000Z","1993-10-17T00:00:00.000Z","1993-10-24T00:00:00.000Z","1993-10-31T00:00:00.000Z","1993-11-07T00:00:00.000Z","1993-11-14T00:00:00.000Z","1993-11-21T00:00:00.000Z","1993-11-28T00:00:00.000Z","1993-12-05T00:00:00.000Z","1993-12-12T00:00:00.000Z","1993-12-19T00:00:00.000Z","1993-12-26T00:00:00.000Z"]
	timestamp = random.choice(timelattice)
	year = timestamp[0:4]
	month = timestamp[5:7] 
	day = timestamp[8:10]
	var = random.choice(['sla', 'adt'])
	nobs_var = {'sla': 'anomaly_nobs', 'adt': 'abs_nobs'}[var]
	dates = timewindow(f"{year}-{month}-{day}T00:00:00Z", 7)
	timeidx = timelattice.index(timestamp)
	try:
		xars = [xarray.open_dataset(f"/tmp/dt_global_twosat_phy_l4_{date}_vDT2021.nc", decode_times=False, mask_and_scale=False) for date in dates]
	except:
		continue
	means = xarray.open_dataset(f"/tmp/sla_adt_mean_{year}.nc", decode_times=False)
	lat = math.floor(random.random()*720)
	lon = math.floor(random.random()*1440)

	total = 0
	nobs = 0
	for xar in xars:
		meas = xar[var][0][lat][lon].to_dict()['data']
		if meas != -2147483647:
			total += meas*0.0001
			nobs += 1

	if means[var][timeidx][lat][lon].to_dict()['data'] != -999.9 and (not math.isclose(total/nobs, means[var][timeidx][lat][lon].to_dict()['data'], abs_tol=1e-5) or not math.isclose(nobs, means[nobs_var][timeidx][lat][lon].to_dict()['data'], abs_tol=1e-5)):
		print(var, timestamp, lat, lon, total/nobs, means[var][timeidx][lat][lon].to_dict()['data'], nobs, means[nobs_var][timeidx][lat][lon].to_dict()['data'])

	means.close()
	for f in xars:
		f.close()