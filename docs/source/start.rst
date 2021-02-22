.. _start-ref:

Getting Started
===============

openclean provides useful functionality to identify bugs and anomalous values, make fixes and wrangle datasets. Here, we walkthrough
a simple example to get you acquainted with openclean in 10 minutes! Our `misspellings dataset <https://github.com/VIDA-NYU/openclean-core/blob/documentation/docs/source/data/misspellings.csv>`_
contains Street, Neighborhood and Borough names for New York City with a bunch of spelling mistakes. The goal of this exercise
is to fix those errors using some tools we have at our disposal.


Loading Data
------------
openclean uses a dataset (a wrapped pandas dataframe) as it's primary data storage object.
It can be created from any source data type accepted by pandas. Compressed Gzip files (.gz) are also accepted.
For large datasets, it might be desirable to perform lazy evaluation on the data instead of loading it all to memory.
To allow this, openclean lets users stream their datasets. More information on Datasets and Streams is provided in the
:ref:`concepts-ref` section.

.. jupyter-execute::

    import os

    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'misspellings.csv'))

    ds.head()


Profiling the Dataset
---------------------
To provide insights about the dataset features, openclean comes with data profiling capabilities. A user can select the default
profiler to get basic statistics or plug in their own profilers for advanced computations. More information about
profilers is available in the :ref:`profile-ref` section.

.. jupyter-execute::

    from openclean.profiling.dataset import dataset_profile

    # generate a profile
    profiles = dataset_profile(ds)

    # see all stats
    profiles.stats()


We see that there exist 11 distinct values in the `Borough` column whereas there are only 5 Boroughs in New York City and
a bunch of them are empty/missing values.

.. jupyter-execute::

    ds['Borough'].value_counts()


Going into further depth, we see realize there are a few variations for `Brooklyn`, `Queens`, `Bronx` and `Manhattan`.


Selecting Columns
-----------------
As discussed earlier, we want to fix the mistakes in the `Borough` column. We can separate this column from the entire dataset using
the select operation. Before we do that, for this example, let's assume we need to get rid of rows that have missing values.
So we'll use the filter operator and the IsNotEmpty Eval function. Eval functions are explained in :ref:`concepts-ref`.

.. jupyter-execute::

    from openclean.operator.transform.filter import filter
    from openclean.function.eval.null import IsNotEmpty

    ds = filter(ds, predicate=IsNotEmpty('Borough'))

    ds['Borough'].value_counts()


Now, let's separate out the column of interest. You can read more on selecting columns and other dataset/stream transformations
in the :ref:`transform-ref` section.

.. jupyter-execute::

    from openclean.operator.transform.select import select

    misspelled_data = select(ds, columns=['Borough'], names=['messy_borough'])

    misspelled_data['messy_borough'].unique()


Downloading and Preparing Master data
-------------------------------------
With openclean, a user can easily incorporate other datasets to enrich the data cleaning process. For e.g., let's download an official
list of borough names from the `Borough Population projections dataset <https://dev.socrata.com/foundry/data.cityofnewyork.us/xywu-7bv9>`_
using Socrata to help us with the wrangling. We shall use this as the ground truth for correct spellings.
You can read more about master datasets in the :ref:`enrich-ref` section.

After downloading the master data, we preprocess it a bit to match the case with our input dataset. We use the update
transformation from :ref:`clean-ref` to achieve this which can accept both: a dictionary or a function as the second argument.

.. jupyter-execute::

    from openclean.data.source.socrata import Socrata
    from openclean.operator.transform.update import update

    # download the master data and select the relevant column
    nyc_boroughs = Socrata().dataset('xywu-7bv9').load()
    nyc_boroughs = select(nyc_boroughs, columns=['Borough'])

    # uppercase and strip the values to match with the misspelled data
    nyc_boroughs = update(nyc_boroughs, 'Borough', str.upper)
    nyc_boroughs = update(nyc_boroughs, 'Borough', str.strip)

    nyc_boroughs


Identifying Fixes
-----------------
We are now familiar with the mistakes in the data and have a master dataset with corrections available. openclean
provides cleaning operators and repair strategies to let users fix their datasets with the minimum amount of coding
involved. A list of various cleaning operators available can be accessed in the :ref:`clean-ref` section.

Here, we calculate Fuzzy String Similarity between `messy_borough` and Master data to create a mapping of misspellings
to the possible fixes.

.. jupyter-execute::

    from openclean.function.matching.base import DefaultStringMatcher
    from openclean.function.matching.fuzzy import FuzzySimilarity
    from openclean.data.mapping import Mapping
    from pprint import pprint

    # the master vocabulary list
    VOCABULARY = nyc_boroughs['Borough']

    # create a string matcher that uses the provided vocabulary and similarity algorithm
    matcher = DefaultStringMatcher(
            vocabulary=VOCABULARY,
            similarity=FuzzySimilarity()
    )

    # create a mapping to store the fixes
    fixes = Mapping()

    # look for matches in the vocabulary
    for query in set(misspelled_data['messy_borough']):
        fixes.add(query, matcher.find_matches(query))

    # print the fixes
    pprint(fixes)


The generated `fixes` mapping contains `messy_borough` content as keys and found matches from the vocabulary along with
a match score as values.

Making Repairs
--------------
The simplest repair strategy here would be to look up `messy_borough` values in the `fixes` map and replace them. We
achieve this with the update transformation from the :ref:`clean-ref` section.

.. jupyter-execute::

    from openclean.operator.transform.update import update

    misspelled_data = update(misspelled_data, 'messy_borough', fixes.to_lookup())

    misspelled_data['messy_borough'].unique()


We fixed it! One can also observe the decrease in uniqueness and entropy.

.. jupyter-execute::

    dataset_profile(misspelled_data).stats()


As we saw in this tiny real world example, openclean makes it straightforward to
not only load and stream datasets, but also to profile them to identify bugs and provide masterdata alongside providing
a toolkit to identify and make fixes.


More Examples
-------------
We provide many other Jupyter notebooks as examples to demonstrate different capabilities of openclean. All our notebooks
along with the used datasets can be found in the :ref:`examples-ref`.