DGGAL DGGRS Provider
--------------------

The DGGRS provider DGGALProvider uses ``dggal 0.0.5`` to generate various DGGRS. The current implementation supports the following grids:

- ISEA7H_Z7
- IVEA7H
- rHEALPix

Initialisation parameters
=========================
The initialisation parameters dictionary: ``parameters`` in the :ref:`dggrs table<dggrs>` provides which grid the instance is going to support.

Users can specify the grid in the ``parameters`` by the keyword ``grid``. For example: 


.. code-block:: json
   :caption: Define the ivea7h grid

	 "dggrs": {"1": 
            {"ivea7h": 
                {"title": "IVEA7H DGGRS provided by dggal",
                 "description": "An equal area hexagonal grid with a refinement ratio of 7 defined in the IVEA projection, using the same global indexing and sub-zone ordering as for ISEA7H", 
                 "crs": "wgs84", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://dggal.org/", 
                 "defaultDepth": 1, 
                 "classname": "dggal_dggrs_provider.DGGALProvider",
                 "parmeters": { "grid": "IVEA7H" }
                }
            }
	}


