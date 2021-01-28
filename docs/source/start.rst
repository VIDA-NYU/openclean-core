Getting Started
===============

openclean provides useful functionality to identify bugs and anomalous values, make fixes and wrangle datasets. To help
illustrate different operations, we use a sample of NYC open data with completed job codes at various locations in New York City.

Loading Data
------------
openclean uses a dataset (a wrapped pandas dataframe) as it's primary data storage object.
It can be created from any source data type accepted by pandas. Compressed Gzip files (.gz) are also accepted.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()


