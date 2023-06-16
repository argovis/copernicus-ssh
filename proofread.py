import datetime, xarray, random, math

def timewindow(center, radius):
    # given a string specifying the central date in the format "1993-02-07T00:00:00.000Z",
    # produce a list of strings for the days +- radius around that date in the format yyyymmdd

    centerdatetime = datetime.datetime.strptime(center, "%Y-%m-%dT%H:%M:%SZ")
    datestrings = []
    for i in range(-1*radius, radius+1):
    	datestrings.append( (centerdatetime + datetime.timedelta(days=i)).strftime("%Y%m%d") )

    return datestrings

dates = timewindow("1993-01-17T00:00:00Z", 3)
timeidx = 1; # time index the timewindow date corresponds to
xars = [xarray.open_dataset(f"data/dt_global_twosat_phy_l4_{date}_vDT2021.nc", decode_times=False, mask_and_scale=False) for date in dates]
means = xarray.open_dataset("data/ssh_mean_1993.nc", decode_times=False)

while True:
	lat = math.floor(random.random()*720)
	lon = math.floor(random.random()*1440)

	total = 0
	nobs = 0
	for xar in xars:
		meas = xar['sla'][0][lat][lon].to_dict()['data']
		if meas != -2147483647:
			total += meas*0.0001
			nobs += 1

	if means['sla'][timeidx][lat][lon].to_dict()['data'] != -999.9 and (not math.isclose(total/nobs, means['sla'][timeidx][lat][lon].to_dict()['data'], abs_tol=1e-5) or not math.isclose(nobs, means['nobs'][timeidx][lat][lon].to_dict()['data'], abs_tol=1e-5)):
		print(total/nobs, means['sla'][timeidx][lat][lon].to_dict()['data'], nobs, means['nobs'][timeidx][lat][lon].to_dict()['data'])