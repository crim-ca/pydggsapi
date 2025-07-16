Clickhouse Collection Provider
==============================
The implementation uses `clickhouse_drive <https://clickhouse-driver.readthedocs.io/en/latest/>`_  to connect to Clickhouse DB. The provider serves multiple tables on the same database, with each table as a data source. It creates an instance of ``clickhouse_diver::Client`` at initialisation and assigns it to ``self.db``. The reference is used in ``get_data`` for data queries. 

Note on Clickhouse query
-------------------------
Clickhouse restricts the query size to 200KB by default. It is controlled by the setting `max_query_size <https://clickhouse.com/docs/operations/settings/settings#max_query_size>`_ . The default size is too small when the number of zone IDs for the query is large. For instance, each zone ID consumes 10 bytes for IGEO7 z7 (in string format) at refinement level 8, the query is limited to 20,000 zones without considering other overheads.


Constructor parameters
----------------------

For ``initla_param`` uses in :ref:`collection_providers <collection_providers>`

* ``host``
* ``user``
* ``password``
* ``port``
* ``compression (default: False)``
* ``database (default: 'default')``

get_data parameters
----------------------

For ``getdata_params`` uses in :ref:`collections <collections>`

* ``table``          : table's name
* ``zoneId_cols``    : a dictionary that maps refinement levels to columns that store the corresponding zone ID. 
* ``data_cols``      : a list of column names to control which columns should be selected for data queries.
* ``aggregation``    : default is 'mode'
* ``max_query_size`` : to be implemented 

A collection example of using clickhouse collection provider :

.. code-block:: json

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

