Abstract DGGRS providers
========================

To implement a DGGRS provider, users need to provide the implementation of the five interfaces listed below: 

- get_zone_level_by_cls
- get_cells_zone_level
- get_relative_zonelevels
- zoneslist
- zonesinfo

DGGRS conversion
----------------

Apart from the mandatory functions listed above, a DGGRS provider can support optional conversion from itself to other DGGRS (target). 

The main idea behind achieving the purpose is straightforward: give a list of original zone IDs, and map the list to the corresponding target zone IDs. For the current implementation, the return of the convert function has to ensure that the length of the original zone ID list is the same as the converted one. The constraint implies that the target refinement level is greater (coarser) than or equal to the source refinement level. 

To implement the conversion feature, the provider has to:

1. Initialise the ``dggrs_conversion`` to register the target dggrs ID with an object instance from ``conversion_properties``. 
2. Implement the convert interface.

The ``conversion_properties`` class stores the necessary conversion info if needed; it is not used anywhere in pydggsapi. 

Implementations
---------------
.. toctree::
   :maxdepth: 2
   :glob:
   :hidden:

   implementations/*






