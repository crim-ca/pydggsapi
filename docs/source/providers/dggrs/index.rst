Abstract DGGRS providers
========================

To implement a DGGRS provider, users need to provide the implementation of the five interfaces listed below: 

- get_cls_by_zone_level
- get_zone_level_by_cls
- get_cells_zone_level
- get_relative_zonelevels
- zone_id_from_textual
- zone_id_to_textual
- zoneslist
- zonesinfo

.. _dggrs_zone_id_repr:

DGGRS zone ID representation 
----------------------------
The API always assumes the zone ID from requests is using the 'textual' representation. The definition of ``textual`` zone ID representation depends on the DGGRS and the DGGRS provider implementation. 

The DGGRS provider should implement the following two functions to support different zone ID representations, both functions take two parameters: ``zone_ids``  and ``zone_id_repr``.

- ``zone_id_from_textual``: converts zone IDs from the ``textual`` format to the representation specified by ``zone_id_repr``. 

- ``zone_id_to_textual``  : converts zone IDs from the representation specified by ``zone_id_repr`` to the ``textual`` format.








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
   :glob:
   :titlesonly:

   implementations/*






