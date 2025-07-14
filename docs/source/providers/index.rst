Abstract providers
=======================

pydggsapi uses the provider concept to modularise different data access methods and DGGRS implementations. We refer them to the ``collection providers`` and ``DGGRS providers``.  

We introduce two abstract classes (``AbstractDGGRSProvider`` and ``AbstractCollectionProvider``) to expose interfaces that interact with pydggsapi. Users can implement new data storage or DGGRS by inheriting from those two abstract classes. To standardise the parameters and returns of interfaces, we use the Pydantic data model to provide the skeleton. 


.. toctree::
   :maxdepth: 2
   :glob:
   :hidden:

   Abstract collection providers <collection_providers/index>
   Abstract DGGRS providers <dggrs/index>






