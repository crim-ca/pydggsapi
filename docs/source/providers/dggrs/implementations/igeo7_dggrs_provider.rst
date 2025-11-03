IGEO7 DGGRS Provider
--------------------

The DGGRS provider IGEO7Provider uses ``dggrid4py (>=0.5.2)`` and ``dggrid (>=8.4.1)`` to generate hexagonal DGGRS using the IGEO7 indexing schema. 

Initialisation parameters
=========================
The initialisation parameters ``parameters`` provided in the :ref:`dggrs table<dggrs>` are stored in the ``self.properties`` object that belongs to the  IGEO7Properties class.

The IGEO7Properties class inherits from the IGEO7MetafileConfig, which stores dggrid metafile configuration.  Currently, it supports the following parameters(default values) for metafile configurations: 

.. code-block:: python
  
   # Z7 index schema setting
   input_address_type: str = 'HIERNDX'
   input_hier_ndx_system: str = 'Z7'
   input_hier_ndx_form: str = 'DIGIT_STRING'
   output_address_type: str = 'HIERNDX'
   output_cell_label_type: str = 'OUTPUT_ADDRESS_TYPE'
   output_hier_ndx_system: str = 'Z7'
   output_hier_ndx_form: str = 'DIGIT_STRING'
   # initial vertex lon setting
   dggs_vert0_lon: float = 11.25

In addition to the metafile setting, the IGEO7Properties class consists of the following attributes related to the setting of the ``dggrid4py``.

.. code-block:: python
   
   geodetic_conversion: bool = False

Geodetic Coordinates System
===========================
IGEO7Provider supports conversion between authalic and geodetic coordinates. It is controlled by the parameter ``geodetic_conversion``. When the setting is True, the provider performs the conversion (geodetic to authalic, authalic to geodetic) for both input coordinates and output returns.

When the ``crs`` setting of the ``dggrs`` is set to ``wgs84``, IGEO7Provider automatically enable the  ``geodetic_conversion`` and set the ``dggs_vert0_lon`` to ``11.20``.


Example of parameter settings
=============================

.. code-block:: json
	:caption: enable geodetic conversion and set the initial vertex lon to 11.20 by setting crs == wgs84

	"dggrs": {"1": 
            {"igeo7": 
                {"title": "IGEO7 DGGRS with z7string",
                 "description": "Hexagonal grid with ISEA projection and refinement ratio of 7. z7 space-filling curve", 
                 "crs": "wgs84", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://agile-giss.copernicus.org/articles/6/32/2025/", 
                 "defaultDepth": 1, 
                 "classname": "igeo7_dggrs_provider.IGEO7Provider",
                }
            }
	}


.. code-block:: json
   :caption: manually enable conversion 

	    "dggrs": {"1": 
            {"igeo7": 
                {"title": "IGEO7 DGGRS with z7string",
                 "description": "Hexagonal grid with ISEA projection and refinement ratio of 7. z7 space-filling curve", 
                 "crs": "Some other CRS", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://agile-giss.copernicus.org/articles/6/32/2025/", 
                 "defaultDepth": 1, 
                 "classname": "igeo7_dggrs_provider.IGEO7Provider",
                 "parmeters": { "geodetic_conversion": true }
                }
            }
	}

