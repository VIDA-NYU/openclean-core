.. _profile-ref:

Data Profiling
==============

openclean comes with pre-configured tools to profile datasets that help to report some actionable metrics. It
also provides a fairly easy to implement interface for users to create/attach their own data profilers. A user can select the default
profiler to get basic statistics (min/max, entropy, distinct values, datatypes etc) or plug in their own profilers for advanced computations.

We use a sample of NYC open data with completed job codes at various locations in New York City to demonstrate some examples.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()

Using the openclean profiler
----------------------------

.. jupyter-execute::

    from openclean.profiling.dataset import dataset_profile

    profiles = dataset_profile(ds)
    print(profiles)

These profiled results can be accessed directly:

.. jupyter-execute::

    # see all stats
    print(profiles.stats())

or can be queried:

.. jupyter-execute::

    # query the minimum and maximum in the Borough column
    print(profiles.minmax('Borough'))

To see different data types in the column:

.. jupyter-execute::

    # look at all the datatypes in the dataset
    print(profiles.types())

We also realize from this that one good exercise to ensure data quality could be to look at the Street Name column's values that have been classified as dates.

Visualizing profiled results
----------------------------

The :ref:`notebook-extension` allows profiled results to be visually seen in the notebook. The following
screen grab demonstrates this using the `Auctus profiler <https://pypi.org/project/datamart-profiler/>`_ with the :ref:`notebook-extension` spreadsheet UI:

.. only:: html

   .. figure:: data/auctus_profiler.gif