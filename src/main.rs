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

    let places = 100000000.0;
    
    //let batchfiles = ["/bulk/copernicus-sla/sla_adt_mean_1993.nc","/bulk/copernicus-sla/sla_adt_mean_1994.nc","/bulk/copernicus-sla/sla_adt_mean_1995.nc","/bulk/copernicus-sla/sla_adt_mean_1996.nc","/bulk/copernicus-sla/sla_adt_mean_1997.nc","/bulk/copernicus-sla/sla_adt_mean_1998.nc","/bulk/copernicus-sla/sla_adt_mean_1999.nc","/bulk/copernicus-sla/sla_adt_mean_2000.nc","/bulk/copernicus-sla/sla_adt_mean_2001.nc","/bulk/copernicus-sla/sla_adt_mean_2002.nc","/bulk/copernicus-sla/sla_adt_mean_2003.nc","/bulk/copernicus-sla/sla_adt_mean_2004.nc","/bulk/copernicus-sla/sla_adt_mean_2005.nc","/bulk/copernicus-sla/sla_adt_mean_2006.nc","/bulk/copernicus-sla/sla_adt_mean_2007.nc","/bulk/copernicus-sla/sla_adt_mean_2008.nc","/bulk/copernicus-sla/sla_adt_mean_2009.nc","/bulk/copernicus-sla/sla_adt_mean_2010.nc","/bulk/copernicus-sla/sla_adt_mean_2011.nc","/bulk/copernicus-sla/sla_adt_mean_2012.nc","/bulk/copernicus-sla/sla_adt_mean_2013.nc","/bulk/copernicus-sla/sla_adt_mean_2014.nc","/bulk/copernicus-sla/sla_adt_mean_2015.nc","/bulk/copernicus-sla/sla_adt_mean_2016.nc","/bulk/copernicus-sla/sla_adt_mean_2017.nc","/bulk/copernicus-sla/sla_adt_mean_2018.nc","/bulk/copernicus-sla/sla_adt_mean_2019.nc","/bulk/copernicus-sla/sla_adt_mean_2020.nc","/bulk/copernicus-sla/sla_adt_mean_2021.nc","/bulk/copernicus-sla/sla_adt_mean_2022.nc"];
    let batchfiles = ["/bulk/copernicus-sla/sla_adt_mean_1993.nc"];

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
        source: Vec<Sourcedoc>,
        tpa_correction: Vec<f64>
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
    let mut tpa_correction = Vec::new();
    for _k in 0..batchfiles.len(){
        let file = netcdf::open(batchfiles[_k])?;
        let timestamps = &file.variable("timestamps").expect("Could not find variable 'timestamps'");
        let tpa_cxn = &file.variable("tpa_correction").expect("Could not find variable 'tpa_correction'");
        for timeidx in 0..timestamps.len() {
            timeseries.push(bson::DateTime::parse_rfc3339_str((t0 + Duration::days(timestamps.value::<i64, _>(timeidx)?)).to_rfc3339().replace("+00:00", "Z")).unwrap() );
        }
        for cxn in 0..tpa_cxn.len() {
            tpa_correction.push((tpa_cxn.value::<f64, _>(cxn)?*places).round()/places);
        }
    }

    let metadata = SlaMetadoc{
        _id: String::from("copernicusSLA"),
        data_type: String::from("sea level anomaly"),
        data_info: (
            vec!(String::from("sla"),String::from("adt"),String::from("ugosa"),String::from("ugos"),String::from("vgosa"),String::from("vgos")),
            vec!(String::from("units"), String::from("long_name")),
            vec!(
                vec!(String::from("m"), String::from("Sea level anomaly")),
                vec!(String::from("m"), String::from("Absolute dynamic topography")),
                vec!(String::from("m/s"), String::from("Geostrophic velocity anomalies: zonal component")),
                vec!(String::from("m/s"), String::from("Absolute geostrophic velocity: zonal component")),
                vec!(String::from("m/s"), String::from("Geostrophic velocity anomalies: meridian component")),
                vec!(String::from("m/s"), String::from("Absolute geostrophic velocity: meridian component"))
            )
        ),
        date_updated_argovis: bson::DateTime::parse_rfc3339_str(nowstring()).unwrap(),
        timeseries: timeseries,
        source: vec!(
            Sourcedoc{
                source: vec!(String::from("Copernicus sea level anomaly")),
                url: String::from("https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-sea-level-global")
            }
        ),
        tpa_correction: tpa_correction
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
        let mut meanugosabatch = Vec::new();
        let mut meanugosbatch = Vec::new();
        let mut meanvgosabatch = Vec::new();
        let mut meanvgosbatch = Vec::new();
        for _lonidx in 0..1440 {
            meanslabatch.push(Vec::new());
            meanadtbatch.push(Vec::new());
            meanugosabatch.push(Vec::new());
            meanugosbatch.push(Vec::new());
            meanvgosabatch.push(Vec::new());
            meanvgosbatch.push(Vec::new());
        }

        for _f in 0..batchfiles.len() { // ie for every year
            let file = netcdf::open(batchfiles[_f])?; 
            let sla = &file.variable("sla").expect("Could not find variable 'sla'");
            let adt = &file.variable("adt").expect("Could not find variable 'adt'");
            let ugosa = &file.variable("ugosa").expect("could not find variable 'ugosa'");
            let ugos = &file.variable("ugos").expect("could not find variable 'ugos'");
            let vgosa = &file.variable("vgosa").expect("could not find variable 'vgosa'");
            let vgos = &file.variable("vgos").expect("could not find variable 'vgos'");
            let sla_nobs = &file.variable("sla_nobs").expect("Could not find variable 'sla_nobs'");
            let adt_nobs = &file.variable("adt_nobs").expect("Could not find variable 'adt_nobs'");
            let ugosa_nobs = &file.variable("ugosa_nobs").expect("Could not find variable 'ugosa_nobs'");
            let ugos_nobs = &file.variable("ugos_nobs").expect("Could not find variable 'ugos_nobs'");
            let vgosa_nobs = &file.variable("vgosa_nobs").expect("Could not find variable 'vgosa_nobs'");
            let vgos_nobs = &file.variable("vgos_nobs").expect("Could not find variable 'vgos_nobs'");
            let timestamps = &file.variable("timestamps").expect("Could not find variable 'timestamps'");

            for lonidx in 0..1440 {
                for timeidx in 0..timestamps.len() {
                    let v_sla = sla.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_sla = sla_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_sla != -999.9 && n_sla == 7 { // ie mask out means that didnt have all 7 days available
                        meanslabatch[lonidx].push((v_sla*places).round()/places);
                    } else {
                        meanslabatch[lonidx].push(NAN);
                    }

                    let v_adt = adt.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_adt = adt_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_adt != -999.9 && n_adt == 7 { // ie mask out adts that didnt have all 7 days available
                        meanadtbatch[lonidx].push((v_adt*places).round()/places);
                    } else {
                        meanadtbatch[lonidx].push(NAN);
                    }

                    let v_ugosa = ugosa.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_ugosa = ugosa_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_ugosa != -999.9 && n_ugosa == 7 { // ie mask out ugosas that didnt have all 7 days available
                        meanugosabatch[lonidx].push((v_ugosa*places).round()/places);
                    } else {
                        meanugosabatch[lonidx].push(NAN);
                    }

                    let v_ugos = ugos.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_ugos = ugos_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_ugos != -999.9 && n_ugos == 7 { // ie mask out ugoss that didnt have all 7 days available
                        meanugosbatch[lonidx].push((v_ugos*places).round()/places);
                    } else {
                        meanugosbatch[lonidx].push(NAN);
                    }

                    let v_vgosa = vgosa.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_vgosa = vgosa_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_vgosa != -999.9 && n_vgosa == 7 { // ie mask out vgosas that didnt have all 7 days available
                        meanvgosabatch[lonidx].push((v_vgosa*places).round()/places);
                    } else {
                        meanvgosabatch[lonidx].push(NAN);
                    }

                    let v_vgos = vgos.value::<f64, _>([timeidx, latidx, lonidx])?;
                    let n_vgos = vgos_nobs.value::<i64, _>([timeidx, latidx, lonidx])?;
                    if v_vgos != -999.9 && n_vgos == 7 { // ie mask out vgoss that didnt have all 7 days available
                        meanvgosbatch[lonidx].push((v_vgos*places).round()/places);
                    } else {
                        meanvgosbatch[lonidx].push(NAN);
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
            let mut nonnans_sla = 0;
            for _i in 0..d_sla.len() {
                if !d_sla[_i].is_nan() {
                    nonnans_sla += 1;
                }
            }

            let d_adt = meanadtbatch[lonidx].clone();
            let mut nonnans_adt = 0;
            for _i in 0..d_adt.len() {
                if !d_adt[_i].is_nan() {
                    nonnans_adt += 1;
                }
            }

            let d_ugosa = meanugosabatch[lonidx].clone();
            let mut nonnans_ugosa = 0;
            for _i in 0..d_ugosa.len() {
                if !d_ugosa[_i].is_nan() {
                    nonnans_ugosa += 1;
                }
            }

            let d_ugos = meanugosbatch[lonidx].clone();
            let mut nonnans_ugos = 0;
            for _i in 0..d_ugos.len() {
                if !d_ugos[_i].is_nan() {
                    nonnans_ugos += 1;
                }
            }

            let d_vgosa = meanvgosabatch[lonidx].clone();
            let mut nonnans_vgosa = 0;
            for _i in 0..d_vgosa.len() {
                if !d_vgosa[_i].is_nan() {
                    nonnans_vgosa += 1;
                }
            }

            let d_vgos = meanvgosbatch[lonidx].clone();
            let mut nonnans_vgos = 0;
            for _i in 0..d_vgos.len() {
                if !d_vgos[_i].is_nan() {
                    nonnans_vgos += 1;
                }
            }

            /// bail out if nothing had anything but nans
            if nonnans_sla + nonnans_adt + nonnans_ugosa + nonnans_ugos + nonnans_vgosa + nonnans_vgos == 0 {
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
                "data": [d_sla, d_adt, d_ugosa, d_ugos, d_vgosa, d_vgos]
            };
            docs.push(data);
        }
        if docs.len() > 0 {
            copernicus_sla.insert_many(docs, None).await?;
        }
    }

    Ok(())
}