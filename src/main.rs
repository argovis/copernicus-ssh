use netcdf;
use chrono::Utc;
use chrono::TimeZone;
use chrono::Duration;
use chrono::Datelike;
use chrono::Timelike;
use chrono::DateTime;
use std::env;
use std::error::Error;

fn tidylon(longitude: f64) -> f64{
    // map longitude on [0,360] to [-180,180], required for mongo indexing
    if longitude <= 180.0{
        return longitude;
    }
    else{
        return longitude-360.0;
    }
}

fn nowstring() -> String{
    // returns a String representing the current ISO8601 datetime

    let now = Utc::now();
    return format!("{}-{:02}-{:02}T{:02}:{:02}:{:02}Z", now.year(), now.month(), now.day(), now.hour(), now.minute(), now.second());
}

// impementing a foreign trait on a forein struct //////////
// per the advice in https://stackoverflow.com/questions/76277096/deconstructing-enums-in-rust/76277117#76277117

struct Wrapper{
    s: String
}

impl std::convert::TryFrom<netcdf::attribute::AttrValue> for Wrapper {
    type Error = &'static str;

    fn try_from(value: netcdf::attribute::AttrValue) -> Result<Self, Self::Error> {

        if let netcdf::attribute::AttrValue::Str(v) = value {
            Ok(Wrapper{s: String::from(v)} )
        } else {
            Err("nope")
        }
    }
}
////////////////////

fn find_basin(basins: &netcdf::Variable, longitude: f64, latitude: f64) -> i32 {    
    let lonplus = (longitude-0.5).ceil()+0.5;
    let lonminus = (longitude-0.5).floor()+0.5;
    let latplus = (latitude-0.5).ceil()+0.5;
    let latminus = (latitude-0.5).floor()+0.5;

    let lonplus_idx = (lonplus - -179.5) as usize;
    let lonminus_idx = (lonminus - -179.5) as usize;
    let latplus_idx = (latplus - -77.5) as usize;
    let latminus_idx = (latminus - -77.5) as usize;

    let corners_idx = [
        // bottom left corner, clockwise
        [latminus_idx, lonminus_idx],
        [latplus_idx, lonminus_idx],
        [latplus_idx, lonplus_idx],
        [latminus_idx, lonplus_idx]
    ];

    let distances = [
        (f64::powi(longitude-lonminus, 2) + f64::powi(latitude-latminus, 2)).sqrt(),
        (f64::powi(longitude-lonminus, 2) + f64::powi(latitude-latplus, 2)).sqrt(),
        (f64::powi(longitude-lonplus, 2) + f64::powi(latitude-latplus, 2)).sqrt(),
        (f64::powi(longitude-lonplus, 2) + f64::powi(latitude-latminus, 2)).sqrt()
    ];

    let mut closecorner_idx = corners_idx[0];
    let mut closedist = distances[0];
    for i in 1..4 {
        if distances[i] < closedist{
            closecorner_idx = corners_idx[i];
            closedist = distances[i];
        }
    }

    match basins.value::<i64,_>(closecorner_idx){
        Ok(idx) => idx as i32,
        Err(e) => panic!("basin problems: {:?} {:#?}", e, closecorner_idx)
    }   
}

fn timewindow(center: &str, radius: i64) -> Vec<String> {
    // given a string specifying the central date in the format "1993-02-07T00:00:00.000Z",
    // produce a list of strings for the days +- radius around that date in the format yyyymmdd

    let rfc3339 = DateTime::parse_from_rfc3339(center).unwrap();
    let mut dates = Vec::new();
    for i in -1*radius..radius+1 {
        let d = rfc3339 + Duration::days(i);
        dates.push(format!("{}{:02}{:02}", d.year(), d.month(), d.day()));
    }

    return dates
    
}

fn main() -> Result<(),netcdf::error::Error> {

    //let dates93 = ["1993-01-10T00:00:00.000Z","1993-01-17T00:00:00.000Z","1993-01-24T00:00:00.000Z","1993-01-31T00:00:00.000Z","1993-02-07T00:00:00.000Z","1993-02-14T00:00:00.000Z","1993-02-21T00:00:00.000Z","1993-02-28T00:00:00.000Z","1993-03-07T00:00:00.000Z","1993-03-14T00:00:00.000Z","1993-03-21T00:00:00.000Z","1993-03-28T00:00:00.000Z","1993-04-04T00:00:00.000Z","1993-04-11T00:00:00.000Z","1993-04-18T00:00:00.000Z","1993-04-25T00:00:00.000Z","1993-05-02T00:00:00.000Z","1993-05-09T00:00:00.000Z","1993-05-16T00:00:00.000Z","1993-05-23T00:00:00.000Z","1993-05-30T00:00:00.000Z","1993-06-06T00:00:00.000Z","1993-06-13T00:00:00.000Z","1993-06-20T00:00:00.000Z","1993-06-27T00:00:00.000Z","1993-07-04T00:00:00.000Z","1993-07-11T00:00:00.000Z","1993-07-18T00:00:00.000Z","1993-07-25T00:00:00.000Z","1993-08-01T00:00:00.000Z","1993-08-08T00:00:00.000Z","1993-08-15T00:00:00.000Z","1993-08-22T00:00:00.000Z","1993-08-29T00:00:00.000Z","1993-09-05T00:00:00.000Z","1993-09-12T00:00:00.000Z","1993-09-19T00:00:00.000Z","1993-09-26T00:00:00.000Z","1993-10-03T00:00:00.000Z","1993-10-10T00:00:00.000Z","1993-10-17T00:00:00.000Z","1993-10-24T00:00:00.000Z","1993-10-31T00:00:00.000Z","1993-11-07T00:00:00.000Z","1993-11-14T00:00:00.000Z","1993-11-21T00:00:00.000Z","1993-11-28T00:00:00.000Z","1993-12-05T00:00:00.000Z","1993-12-12T00:00:00.000Z","1993-12-19T00:00:00.000Z","1993-12-26T00:00:00.000Z"];
    let dates93 = ["1993-01-10T00:00:00.000Z","1993-01-17T00:00:00.000Z","1993-01-24T00:00:00.000Z"];

    // caluclate intervals in days since 1993-01-01 for all timesteps
    let mut timesteps = Vec::new();
    let epoch = DateTime::parse_from_rfc3339("1993-01-01T00:00:00Z").unwrap();
    for _i in 0..dates93.len() {
        let dt = DateTime::parse_from_rfc3339(dates93[_i]).unwrap();
        timesteps.push(dt.signed_duration_since(epoch).num_days());
    }

    // set up a new netcdf file to hold this period's averages
    let mut outfile = netcdf::create("data/xx_ssh_mean_1993.nc")?;
    outfile.add_dimension("latitude", 720)?;
    outfile.add_dimension("longitude", 1440)?;
    outfile.add_dimension("time", dates93.len())?;

    let mut timeidx = 0;

    // vectors to hold interim results
    let mut meanSLAs: Vec<Vec<Vec<f64>>> = vec![vec![vec![-999.9;1440];720];dates93.len()];
    let mut nonFillCount: Vec<Vec<Vec<i32>>> = vec![vec![vec![0;1440];720];dates93.len()];

    for d in dates93 {
        // determine which daily files to average
        let dates = timewindow(d, 3);

        // load upstream data
        for date in dates.iter(){
            let f = netcdf::open(format!("data/dt_global_twosat_phy_l4_{}_vDT2021.nc", date))?;
            let sla = &f.variable("sla").expect("Could not find variable 'sla'");
            for lat in 0..720 {
                for lon in 0..1440 {
                    let v = sla.value::<i64, _>([0, lat, lon])?;
                    if v!= -2147483647 {
                        if meanSLAs[timeidx][lat][lon] == -999.9 {
                            // drop the fill value and start counting real values
                            meanSLAs[timeidx][lat][lon] = 0.0;
                        }
                        meanSLAs[timeidx][lat][lon] += (v as f64) * 0.0001; // account for scale factor here
                        nonFillCount[timeidx][lat][lon] += 1;
                    }
                }
            }
        }

        timeidx += 1;
    }

    // tabulate and record means
    let mut meansla = outfile.add_variable::<f64>("sla",&["time", "latitude", "longitude"])?;
    for time in 0..timeidx{
        for lat in 0..720 {
            let mut msla = Vec::new();
            for lon in 0..1440 {
                if meanSLAs[time][lat][lon] != -999.9 {
                    msla.push(meanSLAs[time][lat][lon] / (nonFillCount[time][lat][lon] as f64));
                } else {
                    msla.push(meanSLAs[time][lat][lon]);
                }
            }
            // write to file
            meansla.put_values(&msla, (time, lat, ..));
        }
    }

    // record how many observations were used to construct each mean
    let mut nobs = outfile.add_variable::<f64>("nobs",&["time", "latitude", "longitude"])?;  // track how many non-fill-value observations the mean is calculated over
    for time in 0..timeidx{
        for lat in 0..720 {
            let mut nobsx = Vec::new();
            for lon in 0..1440 {
                nobsx.push( nonFillCount[time][lat][lon]);
            }
            // write to file
            nobs.put_values(&nobsx, (time, lat, ..));
        }
    }

    // propagate dimensions
    let dates = timewindow(dates93[0], 3);
    /// latitude
    let f = netcdf::open(format!("data/dt_global_twosat_phy_l4_{}_vDT2021.nc", dates[3]))?;
    let latitudes = &f.variable("latitude").expect("Could not find variable 'latitude'");
    let mut lats = Vec::new();
    for lat in 0..720 {
        lats.push(latitudes.value::<f64, _>([lat])?);
    }
    let mut latvals = outfile.add_variable::<f64>("latitude",&["latitude"])?;
    latvals.put_values(&lats, (0));
    /// longitudes
    let longitudes = &f.variable("longitude").expect("Could not find variable 'longitude'");
    let mut lons = Vec::new();
    for lon in 0..1440 {
        lons.push(longitudes.value::<f64, _>([lon])?);
    }
    let mut lonvals = outfile.add_variable::<f64>("longitude",&["longitude"])?;
    lonvals.put_values(&lons, (0));
    /// timestamps
    let mut timestamps = outfile.add_variable::<i64>("timestamps",&["time"])?;
    timestamps.put_values(&timesteps, (0));

    Ok(())
}