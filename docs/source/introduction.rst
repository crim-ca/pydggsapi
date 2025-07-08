Introduction
=======================

pydggsapi runs on python3. It uses FastAPI as the core component to implement the OGC DGGS API standard. pydggsapi aims to provide a flexible structure that can easily extend to support different DGGRS and data storage. The following diagram shows the overall structure of pydggsapi:
|overall_structure|

pydggsapi uses TinyDB(in json format) to store DGGRS and collections information. You can specify the path of the json db in the environment variable ``dggs_api_config``.

This structure provides two abstract class definitions that allow developers to implement new DGGRS and Collection providers easily and quickly to work with pydggsapi. The DGGRS and collections providers serve as accessors to collections. Currently, it supports: 

* DGGRS: `IGEO7 <https://agile-giss.copernicus.org/articles/6/32/2025/>`_ ,  `H3 <https://h3geo.org/>`_

* Collection provider: clickhouse, zarr

Quick setup
-----------
1. setup virtual environment with micromamba file and active it. 

.. code-block:: bash

    micromamba create -n <name>  -f micromamba_env.yaml
    mircomamba activate <name>


In order to work with IGEO7 (using `DGGRIG <https://github.com/sahrk/DGGRID>`_ and python library `dggrid4py <https://github.com/allixender/dggrid4py>`_), the dggrid executable needs to be available. You can compile it yourself, or install into the conda/micromamba environment from conda-forge:


.. code-block:: bash

    micromamba install -c conda-forge dggrid
    pip install dggrid4py


2. run poetry to install dependencies
   
.. code-block:: bash

   poetry install

3. create local .env file from env.sample. Change the environment variables according to your local environment setup. 

.. code-block:: bash
    
    dggs_api_config=<Path to TinyDB>
    DGGRID_PATH=<Path to dggrid executable>

4. Start the server for development: 
   
.. code-block:: bash

   pydggsapi



.. |overall_structure| image:: ./images/pydggsapi_overall_structure.png
   :width: 600
