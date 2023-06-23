## Copernicus sea level anomaly port

This repo contains scripts to process and post the sea level anomaly data found at https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-sea-level-global?tab=overview. This proceeds in two steps: calculating weekly averages from the upstream daily data, and then composing these into timeseries documents in mongodb.

### Part 1: computing daily averages

 - branch: ``main``
 - data preperation: all the daily upstream data from Copernicus (2021 version) in `data/`
 - (to be improved): in ``src/main.rs``, update ``timelattice`` and ``outfile`` per output file (currently in 2 year batches).
 - run in the containerized environment described by ``Dockerfile`` with ``cargo run``.
 - doublecheck results using ``proofread.py`` in the environment defined by ``Dockerfile-proofread``

### Part 2: populating mongodb

 - branch: ``db-population``
 - data preparation: all results from step 1 in ``data/``
 - create empty ``copernicusSLA`` and ``copernicusSLAMeta`` collections with appropriate script in https://github.com/argovis/db-schema
 - run in the containerized environment described by ``Dockerfile`` with ``cargo run``
 - doublecheck results using ``proofread.py`` in environment defined by ``Dockerfile-proofread``
