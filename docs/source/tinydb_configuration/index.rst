Configuration of pydggsapi 
==========================
This section introduces the configuration setup for publishing collections with pydggsapi.

pydggsapi uses TinyDB to store all information it needs in three tables: 

1. collections - the table to store collection info
2. dggrs - the table to store dggrs providers info
3. collection_providers - the table to store collection providers' info

Generally, users mostly work with the ``collections`` table to publish the collection. The record defines a collection with a unique collection ID, metadata, how to access the data (collection provider) and which DGGRS it supports (DGGRS provider).So, to publish a collection through pydggsapi, the users need to provide the following details : 

1. A DGGS-ready dataset. The dataset is converted/regridded into one of the supported DGGRS by pydggsapi.

2. The dggrs ID for the DGGRS.

3. The collection provider ID that is supported by pydggsapi to access the data.

Developers implementing new DGGRS and collections providers must register the latest providers in the tables ``dggrs`` or ``collection_providers`` with a unique ID such that the implementation can be referenced in the collections table.

You can find the details of those three tables in the upcoming sections.




.. _collections:

collections
-----------

Inside the collections table, each document represents a collection. The document comes with a document ID that maps to a dictionary. The dictionary key is the collection ID that maps to a key-value pair of attributes to describe the collection. The following code block shows the table structure with two collections defined.

.. code-block:: json
    :name: _tablestructure

    {
        "collections": { 
            "<document ID>": {
                "<collection ID>" {
                    "attribute1" : "value",
                    "attribute2" : "value",
                }
            },
            "<document ID>": {
                "<collection ID>" {
                    "attribute1" : "value",
                    "attribute2" : "value",
                }
            }
        }
    }

The dictionary associated with the collection ID defines metadata and methods to access the data. 

1. collections ID:  The unique ID for the collection.

2. metadata:  title, description

3. collection_provider: a dictionary that describes how to access the data.

   - providerId: :ref:`the collection provider ID  <collection_providers>`

   - dggrsId: :ref:`the dggrs provider ID <dggrs>`
   
   - maxzonelevel: the maximum refinement level of the data. 
   
   - getdata_params: It depends on which collection provider is in use. It provides detailed parameters for the get_data function implemented by collection providers. Details can be found in the :ref:`Abstract collection providers <parameters_for_get_data>`.

Here is an example on how to define a collection that uses clickhouse as collection provider (i.e. the data is stored in clickhouse DB).

.. code-block:: json
   :name: _collectionexample

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
                                    "data_cols" : ["modelled_fuel_stations","modelled_seashore","modelled_solar_wind",
                                    "modelled_urban_nodes", "modelled_water_bodies", "modelled_gas_pipelines",
                                    "modelled_hydrogen_pipelines", "modelled_corridor_points",  "modelled_powerlines", 
                                    "modelled_transport_nodes", "modelled_residential_areas",  "modelled_rest_areas", 
                                    "modelled_slope"]
                                  }
                        }
                    }
              } 
          }



.. _dggrs:

dggrs
-----

Inside the dggrs table, each document represents a dggrs provider. The document comes with a document ID that maps to a dictionary. The dictionary key is the dggrsId that maps to a key-value pair of attributes to describe the DGGRS. The table structure is the same as the :ref:`collection table <_tablestructure>`.

The dictionary associated with the dggrs ID defines metadata and the actual implementation of the DGGRS. 

    1. dggrs ID : The unique ID for the DGGRS, it is used in the :ref:`dggrsId inside a collection <_collectionexample>`.

    2. metadata : OGC DGGS API required description fields of the DGGRS. (e.g. title, shapeType etc.)

    3. classname : The actual implementation module under dependencies/dggrs_providers

Here is an example on how to define DGGRS for IGEO7 and H3. 

.. code-block:: json

    "dggrs": {"1": 
            {"igeo7": 
                {"title": "IGEO7 DGGRS with z7string",
                 "description": "Hexagonal grid with ISEA projection and refinement ratio of 7. z7 space-filling curve", 
                 "crs": "wgs84", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://agile-giss.copernicus.org/articles/6/32/2025/", 
                 "defaultDepth": 5, 
                 "classname": "igeo7_dggrs_provider.IGEO7Provider" }
            },
            "2": 
            {"h3": 
                {"title": "Uber H3", 
                "description": "Uber H3", 
                "crs": "wgs84", 
                "shapeType": "hexagon", 
                "definition_link": "https://h3geo.org/", 
                "defaultDepth": 5, 
                "classname": "h3_dggrs_provider.H3Provider"}
            }
    }

.. _collection_providers:

collection_providers
--------------------

Inside the collection_providers table, each document represents a collection provider. The document comes with a document ID that maps to a dictionary. The dictionary key is the collection provider ID that maps to a key-value pair of attributes to describe the collection provider. The table structure is the same as the :ref:`collection table <_tablestructure>`.

The dictionary associated with the collection provider ID defines the implementation module and initialization parameters. 

    1. collection provider ID : The unique ID for the collection provider, it is used in the :ref:`providerId inside a collection <_collectionexample>`.

    2. classname : The actual implementation module under dependencies/collections_providers
    
    3. initial_params : A dictionary with parameters to initializ the provider

Here is an example on how to define a collection provier for clickhouse.


.. code-block:: json
   :name: _collection_provider_config_example

    "collection_providers": {"1": 
            {"clickhouse": 
                {"classname": "clickhouse_collection_provider.ClickhouseCollectionProvider", 
                  "initial_params": 
                          {"host": "127.0.0.1", 
                           "user": "user",
                           "password": "password", 
                           "port": 9000, 
                           "database": "DevelopmentTesting"} 
                  }
            }
    }
