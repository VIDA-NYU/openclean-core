Data Enrichment
===============

Socrata
-------
`Socrata <https://dev.socrata.com/data/>`_ data can be downloaded into openclean to be used to enrich datasets and support the data cleaning process. Here we download the
`ITU ICT Development Index (IDI) <https://www.opendatanetwork.com/dataset/idp.nz/3bxy-wfk9>`_ to demonstrate this.

.. jupyter-execute::

    from openclean.data.source.socrata import Socrata

    idi = Socrata().dataset('3bxy-wfk9').load()

    print(idi.head())

Reference Data Repository
-------------------------
The aim of the Reference Data Repository is to provide access to reference data sets (e.g., controlled vocabularies, gazetteers, ...) that are accessible on the Web and that are useful for tools like openclean, DataMart Profiler, D4 etc.

It's still under development but do check again soon for updates!