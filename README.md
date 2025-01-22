# pydggsapi

A python FastAPI OGC DGGS API implementation

## OGC API - Discrete Global Grid Systems

https://ogcapi.ogc.org/dggs/

OGC API - DGGS specifies an API for accessing data organised according to a Discrete Global Grid Reference System (DGGRS). A DGGRS is a spatial reference system combining a discrete global grid hierarchy (DGGH, a hierarchical tessellation of zones to partition) with a zone indexing reference system (ZIRS) to address the globe. Aditionally, to enable DGGS-optimized data encodings, a DGGRS defines a deterministic for sub-zones whose geometry is at least partially contained within a parent zone of a lower refinement level. A Discrete Global Grid System (DGGS) is an integrated system implementing one or more DGGRS together with functionality for quantization, zonal query, and interoperability. DGGS are characterized by the properties of the zone structure of their DGGHs, geo-encoding, quantization strategy and associated mathematical functions.

## Setup and Dependencies

1. setup virtual environment with micromamba file and active it. 

```
micromamba create -n <name>  -f micromamba_env.yaml
mircomamba activate <name>
```

2. run poetry to install dependencies

```
poetry install
```

3. update .env.sample 

```
dggs_api_config=<Path to TinyDB>
DGGRID_PATH=<Path to dggrid executable>
```

4. Start the server: 
```
export POETRY_DOTENV_LOCATION=.env.sample && poetry run python pydggsapi/main.py 
```

## TinyDB Configuration 

(For main branch implementation)

The DB consist of 2 tables to define dggrs providers and collections that will served by the API.

1. collections - describe which dggrs to be supported and how to access the collection
2. dggrs - ogc dggrs description and the implementation class

### Collections 
An example to define a collection : 

The key will be the collection ID, and it consist of : 

1. dggrs_indexes, define which dggrs is supported.
2. provider, define how to access the data 

```
"suitability_hytruck": {
          "dggrs_indexes": {"IGEO7": [5, 6, 7, 8, 9], "H3": [3, 4, 5, 6, 7]},
           "title": "Suitability Modelling for Hytruck", 
           "description": "Desc", 
           "provider": {"providerClassName": "db.Clickhouse", 
                                "providerParams": {"uid": "suitability_hytruck",
                                                                "host": "127.0.0.1", 
                                                                "user": "default", 
                                                                "password": "user", 
                                                                "port": 9000, 
                                                                "database": "DevelopmentTesting", 
                                                                "table": "testing_suitability_IGEO7",
                                                                 "res_cols": {"9":"res_9_id", "8":"res_8_id", "7":"res_7_id", "6":"res_6_id", "5":"res_5_id"}, 
                                                                 "data_cols": ["modelled_fuel_stations","modelled_seashore","modelled_solar_wind","modelled_urban_nodes", 
                                                                 "modelled_water_bodies", "modelled_gas_pipelines", "modelled_hydrogen_pipelines", "modelled_corridor_points", 
                                                                 "modelled_powerlines", "modelled_transport_nodes", "modelled_residential_areas", "modelled_rest_areas",
                                                                 "modelled_slope"]}}}}
```



## Acknowledgments

This software is being developed by the [Landscape Geoinformatics Lab](https://landscape-geoinformatics.ut.ee/expertise/dggs/) of the University of Tartu, Estonia.

This work was funded by the Estonian Research Agency (grant number PRG1764, PSG841), Estonian Ministry of Education and Research (Centre of Excellence for Sustainable Land Use (TK232)), and by the European Union (ERC, [WaterSmartLand](https://water-smart-land.eu/), 101125476 and Interreg-BSR, [HyTruck](https://interreg-baltic.eu/project/hytruck/), #C031).




