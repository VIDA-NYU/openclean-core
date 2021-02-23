.. _clean-ref:

Data Wrangling and Cleaning
===========================
openclean comes with the following operators to help users find anomalies, make fixes and wrangle their datasets.

.. note:: This list is growing and will be updated periodically.

We use a sample of NYC open data with completed job codes at various locations in New York City to demonstrate some examples.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()


Functional Dependency Violations
--------------------------------
openclean makes it easy to identify any `functional dependency <https://opentextbc.ca/dbdesign01/chapter/chapter-11-functional-dependencies/>`_ violations in datasets.

.. jupyter-execute::

    from openclean.operator.map.violations import fd_violations
    from openclean.operator.collector.count import distinct

    fd1_violations = fd_violations(ds, ['Street Name', 'GIS_NTA_NAME'], ['Borough'])

    print('# of violations for FD(Street Name, GIS_NTA_NAME -> Borough) is {}\n'.format(len(fd1_violations)))
    for key, gr in fd1_violations.items():
        print(gr[['Street Name', 'GIS_NTA_NAME', 'Borough']])


We identify one violation in the above example. Clearly, it is row 11 as someone with domain knowledge should be
able to point out that `FD(BROADWAY  SoHo-TriBeCa-Civic Center-Little Italy) -> QUEENS` is incorrect. Let's fix this using
one of the repair strategies available to us:

.. jupyter-execute::

    from openclean.operator.collector.repair import Shortest, Vote, conflict_repair

    # Define the conflict resolution strategy. We use majority vote for both RHS attributes.
    strategy = {'Borough': Vote()}

    # resolve the conflicts
    resolved = conflict_repair(conflicts=fd1_violations, strategy=strategy, in_order=False)

This should replace the violation value with the maximum frequency value of the group and resolve the conflict.

.. jupyter-execute::

    violation_group = resolved[(resolved['Street Name']=='BROADWAY') & (resolved['GIS_NTA_NAME']=='SoHo-TriBeCa-Civic Center-Little Italy')]

    fd2_violations = fd_violations(resolved, ['Street Name', 'GIS_NTA_NAME'], ['Borough'])

    print('# of violations for FD(Street Name, GIS_NTA_NAME -> Borough) is {}\n'.format(len(fd2_violations)))
    print(violation_group)

A complete list of repair strategies can be accessed in the `API Reference <index.html#api-ref>`_


Missing Values
--------------
Depending on the use case, missing values can be handled by the filter transformation (removing them) or by the update
transformation (new values). They are both explained in :ref:`transform-ref`. We demonstrate both again here.

.. jupyter-execute::

    misspelled_data = dataset(os.path.join(path_to_file, 'misspellings.csv'))
    misspelled_data['Borough'].value_counts()


We see there are empty values in this column. First let's update them using a lambda function that uses the is_empty value function.

.. jupyter-execute::

    from openclean.operator.transform.update import update
    from openclean.function.value.null import is_empty

    updated_misspelled = update(misspelled_data, 'Borough', lambda x: 'Unknown' if is_empty(x) else x)

    updated_misspelled['Borough'].value_counts()


We've replaced missing values with `Unknown`. But, because there is no way to be sure of the correct value without
using other columns and data augmentation techniques from the :ref:`enrich-ref` section to get the correct Borough,
we shall filter out the nulls in this example using the IsNotEmpty eval function.

.. jupyter-execute::

    from openclean.operator.transform.filter import filter
    from openclean.function.eval.null import IsNotEmpty

    misspelled_data = filter(misspelled_data, predicate=IsNotEmpty('Borough'))

    misspelled_data['Borough'].value_counts()

The whole range of eval functions and value functions can be accessed in the `Eval package of the API Reference <api/openclean.function.eval.html>`_
and `Value package of the API Reference <api/openclean.function.value.html>`_ respectively as explained in :ref:`concepts-ref`.


Misspellings and Data Entry Bugs
--------------------------------
openclean can help identify misspellings and data entry bugs using it's powerful string matcher class. It helps
detect anomalous values using:

* Phonetic Matching
    Phonetic algorithms transform the input strings to normalized phonetic encodings before comparing them. openclean has the following phonetic string algorithms:

    * NYSIIS

    * Soundex

    * Metaphone

* Fuzzy Matching
    Implementation of fuzzy string matching using n-gram overlaps and levenshtein or cosine distance.

StringMatcher objects ingest a vocabulary, and a matching algorithm that is used to identify dataset values that are misspelled. These can optionally be stored into
an openclean mapping to be reused later with other datasets as translation tables.

.. jupyter-execute::

    from openclean.function.matching.base import DefaultStringMatcher
    from openclean.function.matching.fuzzy import FuzzySimilarity
    from openclean.data.mapping import Mapping

    VOCABULARY = ['BROOKLYN' ,'MANHATTAN','STATEN ISLAND','BRONX', 'QUEENS']

    matcher = DefaultStringMatcher(
        vocabulary=VOCABULARY,
        similarity=FuzzySimilarity()
    )

    map = Mapping()
    for query in set(misspelled_data['Borough']):
        map.add(query, matcher.find_matches(query))

    print(map)

The map shows all misspellings matched at least one value from the vocabulary so the map can be used to fix the `Borough` column.
The user will have to manually intervene and update the map if for a query value there were zero or more than one matches from the vocabulary.

Fixing is easy, we can use the update operation with the Lookup eval function (to provide default values if key not found in the map).

.. jupyter-execute::

    from openclean.function.eval.domain import Lookup
    from openclean.operator.transform.update import update
    from openclean.function.eval.base import Col


    fixed = update(misspelled_data, 'Borough', Lookup(columns=['Borough'], mapping=map.to_lookup(), default=Col('Borough')))

    print(fixed['Borough'].unique())

* KNN Clustering
    openclean lets users use KNN clustering to identify values in a dataset that are potential variations of one another, and suggests a replacement for each. For e.g. those with missing or extra punctuation. A well-put example demonstrating this can be found `here <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/nyc-restaurant-inspections/NYC%20Restaurant%20Inspections%20-%20kNN%20Clusters%20for%20Business%20Name.ipynb>`_.


Data Standardization
--------------------
Many a times, users will be faced with situations where the dataset contains variations of spellings (with extra / missing punctuations) for example, a Business or a street name. openclean provides multiple ways to enforce consistency across datasets:

* Token Signature Outliers
    When values in the dataset are expected to have a signature token present, this functionality helps identify anomalies. For e.g. an address column often requires values to contain `street suffixes <https://pe.usps.com/text/pub28/28apc_002.htm>`_. The Token Signature class will ensure any values that don't have one are highlighted. `Here <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/parking-violations/Parking%20Violations%20-%20Token%20Signature%20Outliers%20for%20Street%20Names.ipynb>`_ is a notebook demonstrating this.

* KNN Clustering
    As mentioned in the previous section, KNN Clustering can be used to identify sets of similar values. openclean takes it up a notch by recommending a suggested value for each identified cluster.

These two tools become even more powerful when synergized with each other as demonstrated `in this notebook <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/parking-violations/Parking%20Violations%20-%20Key%20Collision%20Clustering%20for%20Street%20Names.ipynb>`_ where
we use first replace different token signature representations with a standard one and then use KNN Clustering to fix the possible
value variations in a street name column.


Statistical Outliers
--------------------
openclean provides many statistical anomaly detection operators that are implemented by the scikit-learn machine learning library.
To name them, we have:

* `DBSCAN <https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html>`_
* `Isolation Forests <https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html>`_
* `Local Outlier Factors <https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.LocalOutlierFactor.html>`_
* `One Class SVM <https://scikit-learn.org/stable/modules/generated/sklearn.svm.OneClassSVM.html>`_
* `Robust Covariance <https://scikit-learn.org/stable/auto_examples/covariance/plot_mahalanobis_distances.html>`_

Here we use a simple ensemble approach that applies all these operators to the dataset's GIS_NTA_NAME column.

.. jupyter-execute::

    from collections import Counter

    ensemble = Counter()

    from openclean.embedding.feature.default import UniqueSetEmbedding
    from openclean.profiling.anomalies.sklearn import (
        dbscan,
        isolation_forest,
        local_outlier_factor,
        one_class_svm,
        robust_covariance
    )

    for f in [dbscan, isolation_forest, local_outlier_factor, one_class_svm, robust_covariance]:
        ensemble.update(f(ds, 'GIS_NTA_NAME', features=UniqueSetEmbedding()))

We then count for each value, the number of operators that classified the value as an outlier.

.. jupyter-execute::

    # Output values that have been classified as outliers by at least three out of the
    # five operators.

    prev = 0
    for value, count in ensemble.most_common():
        if count < 3:
            break
        if count < prev:
            print()
        if count != prev:
            print('{}\t{}'.format(count, value))
        else:
            print('\t{}'.format(value))
        prev = count

Statistically classified as anomalies, these neighborhoods can be those with fewer job requests or misspellings. Something a user
with domain knowledge can verify.

.. _custom-func-ref:

Custom functions
----------------
A user can create their own data cleaning operators, apply them and reuse them as per their requirements.
With :ref:`notebook-extension`, these eval functions or callables can further be registered on a UI and applied to
datasets visually. The following screen grab shows how custom functions together with
openclean-notebook enhance a user's data munging experience:

.. only:: html

   .. figure:: data/custom_func.gif
