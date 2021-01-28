Data Enrichment
===============

Masterdata using Socrates
-------------------------
Masterdata can be downloaded into openclean to enrich datasets and support the data cleaning process. In this quick example,
we download the `ITU ICT Development Index (IDI) <https://www.opendatanetwork.com/dataset/idp.nz/3bxy-wfk9>`_  from `Socrata <https://dev.socrata.com/data/>`_ to demonstrate this.

.. jupyter-execute::

    from openclean.data.source.socrata import Socrata

    idi = Socrata().dataset('3bxy-wfk9').load()

    print(idi.head())

Masterdata using Reference Data Repository
------------------------------------------
openclean integrates the refdata package to provides easy access to several different reference datasets that are
available online for download. Reference datasets are for example a great source for lookup tables and mappings that
are used in data cleaning for outlier detection and data standardization.

There are a couple of examples in the `Masterdata guide <examples.html#examples>`_ which show how refdata package
can be used to get masterdata for cleaning operations.

For more information, `visit the official repo <https://www.github.com/VIDA-NYU/reference-data-repository/>`_
