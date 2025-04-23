# pydggsapi

A python FastAPI OGC DGGS API implementation

## OGC API - Discrete Global Grid Systems

https://ogcapi.ogc.org/dggs/

OGC API - DGGS specifies an API for accessing data organised according to a Discrete Global Grid Reference System (DGGRS). A DGGRS is a spatial reference system combining a discrete global grid hierarchy (DGGH, a hierarchical tessellation of zones to partition) with a zone indexing reference system (ZIRS) to address the globe. Aditionally, to enable DGGS-optimized data encodings, a DGGRS defines a deterministic for sub-zones whose geometry is at least partially contained within a parent zone of a lower refinement level. A Discrete Global Grid System (DGGS) is an integrated system implementing one or more DGGRS together with functionality for quantization, zonal query, and interoperability. DGGS are characterized by the properties of the zone structure of their DGGHs, geo-encoding, quantization strategy and associated mathematical functions.

## Setup and Dependencies

1. switch to restructur_of_providers branch

```
git clone https://github.com/LandscapeGeoinformatics/pydggsapi.git
git switch restructur_of_providers
```

2. setup virtual environment with micromamba file and active it. 

```
micromamba create -n <name>  -f micromamba_env.yaml
mircomamba activate <name>
```

3. run poetry to install dependencies

```
poetry install
```

4. update .env.sample 

```
dggs_api_config=<Path to TinyDB>
DGGRID_PATH=<Path to dggrid executable>
```

5. Start the server: 
```
export POETRY_DOTENV_LOCATION=.env.sample && poetry run python pydggsapi/main.py 
```

## Mini Howto (restructure_of_providers)


### Collections, Collection Providers and DGGRS providers

The are two parts of configurations. 

User Configurations:

 - Collections : to define a collection with meta data, how to access the data (collection provider) and which dggrs it support (dggrs provider).

System configurations:

 - Collection Providers : Implementation to access the data.
 - DGGRS  providers :  Implementation to support API endpoint operations for DGGS

Each data collection must be already formatted  in one of the supported DGGRS implementation (ie. at least one columns to represent the zone ID)

#### An example for Collections definition (in TinyDB): 

The below example on collections defines : 

1. The collections ID (suitability_hytruck) 
2. meta data (title, description) 
3. collection provider : 

     - providerId          : the collection provider id that defined in [collection_providers section](#collection_provider_id)
     - dggrsId               : the dggrs ID that defined in [dggrs section](#dggrs_provider_id). It indicate the dggrs that comes with the data.
     - maxzonelevel    : the maximum refinement level that is support by the data with the dggrs defined above.
     - getdata_params :  it is collections provider specific, It use to provide details parameters for the get_data function implemented by collection providers.
```
"collections": {"1": 
				{"suitability_hytruck": 
					{"title": "Suitability Modelling for Hytruck",
				 	 "description": "Desc", 
				  	"collection_provider": {
				  			"providerId": "clickhouse", 
				  			"dggrsId": "igeo7",
				  			 "maxzonelevel": 9,
				  			 "getdata_params": 
				  			 	{ "table": "testing_suitability_IGEO7", 
				  			 	   "zoneId_cols": {"9":"res_9_id", "8":"res_8_id", "7":"res_7_id", "6":"res_6_id", "5":"res_5_id"},
				  			 	   "data_cols" ["modelled_fuel_stations","modelled_seashore","modelled_solar_wind",
				  			 	   "modelled_urban_nodes", "modelled_water_bodies", "modelled_gas_pipelines",
				  			 	   "modelled_hydrogen_pipelines", "modelled_corridor_points",  "modelled_powerlines", 
				  			 	   "modelled_transport_nodes", "modelled_residential_areas",  "modelled_rest_areas", 
				  			 	   "modelled_slope"]
				  			 	 }
				  		}
				  	}
				} 
			}
```

#### An example for Collection Providers definition (in TinyDB): 

The following configuration defines a collection provider with : 

<a name="collection_provider_id"></a>
1. collection provider ID : clickhouse (this will be used in the collections config under the collection_provider section)

2. classname : ["db\.Clickhouse"](pydggsapi/dependencies/collections_providers/db.py) the implementation class info (under [dependencies/collections_providers folder](pydggsapi/dependencies/collections_providers))

3. initial_params : parameters to initializing the class

```
"collection_providers": {"1": 
		{"clickhouse": 
			{"classname": "db.Clickhouse", 
			  "initial_params": 
			  		{"host": "127.0.0.1", 
			  		 "user": "default",
			  		 "password": "user", 
			  		 "port": 9000, 
			  		 "database": "DevelopmentTesting"} 
			  }
		}
}
```


**Collection provider - Zarr**

Collection provider to support Zarr data format with Xarray DataTree.

- The Zarr collection provider uses xarray to support Zarr data format
- Each refinement level (resolution) is treated as a group in Zarr
- It will return all data variables
- It holds a dictionary to the xarray object for each data source.
- Data sources (folder path) can be specified in either:
     - initial_params will load the data source at the start.
     - Collection's getdata_params will load the data source at run-time.


Data source defined in init_params
```
"collection_providers": {"2": 
		{"zarr": 
			{"classname": "filebase.Zarr", 
			  "initial_params": { 
			  			"datasources": {
			  						"my_zarr_data": {
			  									  "filepath": "<path to zarr folder>",
			  									 "zones_grps" : { "4": "res4", "5": "res5"}
			  					      } 
			  		 	} 
			  		 }
			  }
		}
}
```


Data source defined in Collections
```
"collections": {"2": 
				{"suitability_hytruck_zarr": 
					{"title": "Suitability Modelling for Hytruck for Zarr Data format",
				 	 "description": "Desc", 
				  	"collection_provider": {
				  			"providerId": "zarr", 
				  			"dggrsId": "igeo7",
				  			 "maxzonelevel": 9,
				  			 "getdata_params": { 
				  			 		datasource_id: "my_zarr_data",
			  						"filepath": "<path to zarr folder>",
			  						"zones_grps" : { "4": "res4", "5": "res5"}
			  					} 
				  			 }
				  		}
				  	}
				} 
			}
```


#### An example for DGGRS providers definition (in TinyDB): 

The following configuration defines a dggrs provider with : 

<a name="dggrs_provider_id"></a>
1. dggrs provider ID : igeo7 and h3 (this will be used in the collections config under the collection_provider section)

2. ogc dggs API required descriptions for dggrs. (ex. title, shapeType etc.)

2. classname : "igeo\.IGEO7", "h3\.H3" the implementation class info (under [dependencies/dggrs_providers folder](pydggsapi/dependencies/dggrs_providers))

```
"dggrs": {"1": 
		{"igeo7": 
			{"title": "ISEA7H z7string",
			 "description": "desc", 
			 "crs": "wgs84", 
			 "shapeType": "hexagon", 
			 "definition_link": "http://testing", 
			 "defaultDepth": 5, 
			 "classname": "igeo7.IGEO7" }
		},
		"2": 
		{"h3": 
			{"title": "h3", 
			"description": "desc", 
			"crs": "wgs84", 
			"shapeType": "hexagon", 
			"definition_link": "http://h3test", 
			"defaultDepth": 5, 
			"classname": "h3.H3"}
		}
}
```




## Acknowledgments

This software is being developed by the [Landscape Geoinformatics Lab](https://landscape-geoinformatics.ut.ee/expertise/dggs/) of the University of Tartu, Estonia.

This work was funded by the Estonian Research Agency (grant number PRG1764, PSG841), Estonian Ministry of Education and Research (Centre of Excellence for Sustainable Land Use (TK232)), and by the European Union (ERC, [WaterSmartLand](https://water-smart-land.eu/), 101125476 and Interreg-BSR, [HyTruck](https://interreg-baltic.eu/project/hytruck/), #C031).




