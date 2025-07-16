Providers
=======================
pydggsapi uses the provider concept to modularise different data access methods and DGGRS implementations. We refer them to the ``collection providers`` and ``DGGRS providers``.  

Life cycle of providers
-----------------------
pydggsapi will create an object instance for each provider when it starts and serve for data queries. Data sources with the same DGGRS or collection provider share the same object instance. Thus, those implementations should be stateless. 

Abstract providers
-----------------------
We introduce two abstract classes (``AbstractDGGRSProvider`` and ``AbstractCollectionProvider``) to expose interfaces that interact with pydggsapi. Users can implement new data storage or DGGRS by inheriting from those two abstract classes. To standardise the parameters and returns of interfaces, we use the Pydantic data model to provide the skeleton. 


.. toctree::
   :glob:
   :hidden:

   Abstract Collection providers <collection_providers/index>
   Abstract DGGRS providers <dggrs/index>






