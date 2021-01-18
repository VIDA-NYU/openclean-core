Data Enrichment
===============

Masterdata
----------
Masterdata can be downloaded into openclean to enrich datasets and support the data cleaning process. In this quick example,
we download the `ITU ICT Development Index (IDI) <https://www.opendatanetwork.com/dataset/idp.nz/3bxy-wfk9>`_  from `Socrata <https://dev.socrata.com/data/>`_ to demonstrate this.

.. jupyter-execute::

    from openclean.data.source.socrata import Socrata

    idi = Socrata().dataset('3bxy-wfk9').load()

    print(idi.head())

There are many examples in the `Masterdata guide <examples.html#examples>`_ which shows all the data repositories openclean currently supports.

Reference Data Repository
-------------------------
The aim of the Reference Data Repository is to provide access to reference data sets (e.g., controlled vocabularies, gazetteers, ...) that are accessible on the Web and that are useful for tools like openclean, DataMart Profiler, D4 etc.

It's still under development but do check again soon for updates!