use netcdf;
use chrono::Utc;
use chrono::TimeZone;
use chrono::Duration;
use chrono::Datelike;
use chrono::Timelike;
use std::env;
use std::f64::NAN;
use std::error::Error;
use mongodb::{Client, options::{ClientOptions, ResolverConfig}};
use tokio;
use mongodb::bson::{doc};
use serde::{Deserialize, Serialize};
use mongodb::bson::DateTime;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // fixed coordinates
    
    let batchfiles = ["/bulk/copernicus-sla/sla_adt_mean_1993.nc","/bulk/copernicus-sla/sla_adt_mean_1994.nc","/bulk/copernicus-sla/sla_adt_mean_1995.nc","/bulk/copernicus-sla/sla_adt_mean_1996.nc","/bulk/copernicus-sla/sla_adt_mean_1997.nc","/bulk/copernicus-sla/sla_adt_mean_1998.nc","/bulk/copernicus-sla/sla_adt_mean_1999.nc","/bulk/copernicus-sla/sla_adt_mean_2000.nc","/bulk/copernicus-sla/sla_adt_mean_2001.nc","/bulk/copernicus-sla/sla_adt_mean_2002.nc","/bulk/copernicus-sla/sla_adt_mean_2003.nc","/bulk/copernicus-sla/sla_adt_mean_2004.nc","/bulk/copernicus-sla/sla_adt_mean_2005.nc","/bulk/copernicus-sla/sla_adt_mean_2006.nc","/bulk/copernicus-sla/sla_adt_mean_2007.nc","/bulk/copernicus-sla/sla_adt_mean_2008.nc","/bulk/copernicus-sla/sla_adt_mean_2009.nc","/bulk/copernicus-sla/sla_adt_mean_2010.nc","/bulk/copernicus-sla/sla_adt_mean_2011.nc","/bulk/copernicus-sla/sla_adt_mean_2012.nc","/bulk/copernicus-sla/sla_adt_mean_2013.nc","/bulk/copernicus-sla/sla_adt_mean_2014.nc","/bulk/copernicus-sla/sla_adt_mean_2015.nc","/bulk/copernicus-sla/sla_adt_mean_2016.nc","/bulk/copernicus-sla/sla_adt_mean_2017.nc","/bulk/copernicus-sla/sla_adt_mean_2018.nc","/bulk/copernicus-sla/sla_adt_mean_2019.nc","/bulk/copernicus-sla/sla_adt_mean_2020.nc","/bulk/copernicus-sla/sla_adt_mean_2021.nc","/bulk/copernicus-sla/sla_adt_mean_2022.nc"];

    // mongodb setup ////////////////////////////////////////////////////////////
    // Load the MongoDB connection string from an environment variable:
    let client_uri =
       env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!"); 

    // A Client is needed to connect to MongoDB:
    // An extra line of code to work around a DNS issue on Windows:
    let options =
       ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
          .await?;
    let client = Client::with_options(options)?; 

    // collection objects
    let copernicus_sla = client.database("argo").collection("copernicusSLA");
    let copernicus_sla_meta = client.database("argo").collection("timeseriesMeta");
    let summaries = client.database("argo").collection("summaries");

    // Rust structs to serialize time properly
    #[derive(Serialize, Deserialize, Debug)]
    struct Sourcedoc {
        source: Vec<String>,
        url: String
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct SlaMetadoc {
        _id: String,
        data_type: String,
        data_info: (Vec<String>, Vec<String>, Vec<Vec<String>>),
        date_updated_argovis: DateTime,
        timeseries: Vec<DateTime>,
        source: Vec<Sourcedoc>
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct summaryDoc {
        _id: String,
        data: Vec<String>,
        longitude_grid_spacing_degrees: f64,
        latitude_grid_spacing_degrees: f64,
        longitude_center: f64,
        latitude_center: f64
    }

    /////////////////////////////////////////////////////////////////////////////////

    // metadata construction

    // all times recorded as days since Jan 1 1993
    let t0 = Utc.with_ymd_and_hms(1993, 1, 1, 0, 0, 0).unwrap();

    let mut timeseries = Vec::new();
    for _k in 0..batchfiles.len(){
        let file = netcdf::open(batchfiles[_k])?;
        let timestamps = &file.variable("timestamps").expect("Could not find variable 'timestamps'");
        for timeidx in 0..timestamps.len() {
            timeseries.push(bson::DateTime::parse_rfc3339_str((t0 + Duration::days(timestamps.value::<i64, _>(timeidx)?)).to_rfc3339().replace("+00:00", "Z")).unwrap() );
        }
    }

    let metadata = SlaMetadoc{
        _id: String::from("copernicusSLA"),
        data_type: String::from("sea level anomaly"),
        data_info: (
            vec!(String::from("sla"),String::from("adt")),
            vec!(String::from("units"), String::from("long_name")),
            vec!(
                vec!(String::from("m"), String::from("Sea level anomaly")),
                vec!(String::from("m"), String::from("Absolute dynamic topography")),
            )
        ),
        date_updated_argovis: bson::DateTime::parse_rfc3339_str(nowstring()).unwrap(),
        timeseries: timeseries,
        source: vec!(
            Sourcedoc{
                source: vec!(String::from("Copernicus sea level anomaly")),
                url: String::from("https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-sea-level-global")
            }
        )
    };
    let metadata_doc = bson::to_document(&metadata).unwrap();
    copernicus_sla_meta.insert_one(metadata_doc.clone(), None).await?;

    // construct summary doc
    let summary = summaryDoc {
        _id: String::from("copernicusslasummary"),
        data: vec!(String::from("sla"), String::from("adt")),
        longitude_grid_spacing_degrees: 0.25,
        latitude_grid_spacing_degrees: 0.25,
        longitude_center: 0.125,
        latitude_center: 0.125
    };
    let summary_doc = bson::to_document(&summary).unwrap();
    summaries.insert_one(summary_doc.clone(), None).await?;

    // data doc: start by building matrix of measurement values for a single latitude and all the longitudes:

    // basin lookup
    let basinfile = netcdf::open("/bulk/copernicus-sla/basinmask_01.nc")?;
    let basins = &basinfile.variable("BASIN_TAG").expect("Could not find variable 'BASIN_TAG'");

    for latidx in 0..720 {
        println!("latindex {}", latidx);
        let mut meanslabatch = Vec::new();
        let mut meanadtbatch = Vec::new();
        for _lonidx in 0..1440 {
            meanslabatch.push(Vec::new());
            meanadtbatch.push(Vec::new());
        }

        for _f in 0..batchfiles.len() { // ie for every year
            let file = netcdf::open(batchfiles[_f])?; 
            let sla = &file.variable("sla").expect("Could not find variable 'sla'");
            let adt = &file.variable("adt").expect("Could not find variable 'adt'");
            let sla_nobs = &file.variable("anomaly_nobs").expect("Could not find variable 'anomaly_nobs'");
            let adt_nobs = &file.variable("abs_nobs").expect("Could not find variable 'abs_nobs'");
            let timestamps = &file.variable("timestamps").expect("Could not find variable 'timestamps'");

            for lonidx in 0..1440 {
                for timeidx in 0..timestamps.len() {
                    let v = sla.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n = sla_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v != -999.9 && n == 7 { // ie mask out means that didnt have all 7 days available
                        meanslabatch[lonidx].push(v);
                    } else {
                        meanslabatch[lonidx].push(NAN);
                    }

                    let v_adt = adt.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_adt = adt_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_adt != -999.9 && n_adt == 7 { // ie mask out adts that didnt have all 7 days available
                        meanadtbatch[lonidx].push(v_adt);
                    } else {
                        meanadtbatch[lonidx].push(NAN);
                    }
                }
            }
        }

        // construct all the json docs and dump to a file
        let mut docs = Vec::new();
        let file = netcdf::open(batchfiles[0])?;
        let latitude = &file.variable("latitude").expect("Could not find variable 'latitude'");
        let longitude = &file.variable("longitude").expect("Could not find variable 'longitude'");
        for lonidx in 0..1440 {
            let d_sla = meanslabatch[lonidx].clone();
            /// bail out if the whole timeseries is nan
            let mut nonnans_sla = 0;
            for _i in 0..d_sla.len() {
                if !d_sla[_i].is_nan() {
                    nonnans_sla += 1;
                }
            }
            if nonnans_sla == 0 {
                continue;
            }

            let d_adt = meanadtbatch[lonidx].clone();
            /// bail out if the whole timeseries is nan
            let mut nonnans_adt = 0;
            for _i in 0..d_adt.len() {
                if !d_adt[_i].is_nan() {
                    nonnans_adt += 1;
                }
            }
            if nonnans_adt == 0 {
                continue;
            }

            let lat = latitude.value::<f64, _>([latidx])?;
            let lon = tidylon(longitude.value::<f64, _>([lonidx])?);
            let basin = find_basin(&basins, lon, lat);
            let id = [lon.to_string(), lat.to_string()].join("_");
            let data = doc!{
                "_id": id,
                "metadata": ["copernicusSLA"],
                "basin": basin,
                "geolocation": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "data": [d_sla, d_adt]
            };
            docs.push(data);
        }
        if docs.len() > 0 {
            copernicus_sla.insert_many(docs, None).await?;
        }
    }

    Ok(())
}