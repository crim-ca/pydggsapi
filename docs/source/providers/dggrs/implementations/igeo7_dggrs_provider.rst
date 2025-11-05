IGEO7 DGGRS Provider
--------------------

The DGGRS provider IGEO7Provider uses ``dggrid4py (>=0.5.2)`` and ``dggrid (>=8.4.1)`` to generate hexagonal DGGRS using the IGEO7 indexing schema. 

Initialisation parameters
=========================
The initialisation parameters ``parameters`` provided in the :ref:`dggrs table<dggrs>` are stored in the ``self.properties`` object that belongs to the  IGEO7MetafileConfig class.

The IGEO7MetafileConfig stores dggrid metafile configuration.  Currently, it supports the following parameters(default values) for metafile configurations: 

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
   dggs_vert0_lon: decimal.Decimal | float | str = 11.20
   # The following parameters are included to let users know what value is used.
   # It should not be channged.
   dggs_vert0_lat: Final[decimal.Decimal | float | str] = 58.28252559
   dggs_vert0_azimuth: Final[decimal.Decimal | float | str] = 0.0



WGS84 Geodetic Coordinates System
=================================
IGEO7Provider supports conversion between authalic and WGS84 geodetic coordinates. It is controlled by the ``crs`` of the dggrs. The conversion is enable if and only if the ``crs == wgs84``. When it is enabled, the provider performs the conversion for both input coordinates and output returns.


Example of parameter settings
=============================

.. code-block:: json
 	:caption: enable WGS84 geodetic conversion and use `11.20` for the `dggs_vert0_lon` by default

	"dggrs": {"1": 
             {"igeo7": 
                {"title": "IGEO7 DGGRS with z7string",
                 "description": "Hexagonal grid with ISEA projection and refinement ratio of 7. z7 space-filling curve", 
                 "crs": "wgs84", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://agile-giss.copernicus.org/articles/6/32/2025/", 
                 "defaultDepth": 1, 
                 "classname": "igeo7_dggrs_provider.IGEO7Provider"
                 }
            }
	}


.. code-block:: json
   :caption: manually set the `dggs_vert0_lon` to 11.25 and disable WGS84 geodetic conversion

	    "dggrs": {"1": 
            {"igeo7": 
                {"title": "IGEO7 DGGRS with z7string",
                 "description": "Hexagonal grid with ISEA projection and refinement ratio of 7. z7 space-filling curve", 
                 "crs": "authalic", 
                 "shapeType": "hexagon", 
                 "definition_link": "https://agile-giss.copernicus.org/articles/6/32/2025/", 
                 "defaultDepth": 1, 
                 "classname": "igeo7_dggrs_provider.IGEO7Provider",
                 "parmeters": { "dggs_vert0_lon": 11.25 }
                }
            }
	}

