Abstract Collection providers
=============================

To implement a collection provider, users need to provide the implementation of the interface listed below: 

- ``get_data``: implementation of the data query from the dataset
- ``get_datadictionary``: implementation of getting the data dictionary (column names and data types) from the dataset, for the tiles JSON response.

Class constructor
-----------------

The :ref:`collection_providers <collection_providers>` configuration provides a parameters dictionary with the key ``inital_params`` to supply necessary info when initialising the collection provider.  Users can reference the full example :ref:`here <_collection_provider_config_example>`.

.. code-block:: json
    
    "initial_params":
                       {"host": "127.0.0.1",
                        "user": "user",
                        "password": "password",
                        "port": 9000,
                        "database": "DevelopmentTesting"} 


.. _parameters_for_get_data:

Parameters for get_data
-----------------------
The pydggsapi creates collection provider objects at the beginning, and data sources that share the same provider will use the same object instance. Thus, in addition to the standard parameters of the interface ``get_data``,  pydggsapi will pass in a parameters dictionary ``getdata_params`` defined in the :ref:`collections <collections>` setting. The extra parameters provide flexibility for the get_data interface if needed.


.. code-block:: json
   
   "getdata_params":
                    { "table": "testing_suitability_IGEO7",
                      "zoneId_cols": {"9":"res_9_id", "8":"res_8_id", "7":"res_7_id", "6":"res_6_id", "5":"res_5_id"},
                      "data_cols" : ["modelled_fuel_stations","modelled_seashore","modelled_solar_wind"]
                    }

Parameters for get_datadictionary
---------------------------------
The pydggsapi passes in the ``getdata_params`` to this function, so developers can reuse the parameters to determine which data source should be used to provide the data dictionary.

.. _collection_providers_implementation:

Implementations
---------------
.. toctree::
   :glob:
   :titlesonly:

   implementations/*








