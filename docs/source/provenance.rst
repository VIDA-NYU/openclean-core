.. _provenance-ref:

Data Provenance
===============
openclean provides users the ability to maintain provenance of the operations performed on a dataset. Just like
a version control system, it has methods to load, commit, and checkout versions of the dataset. Here we talk about
the methods available to achieve this.

We use a sample of NYC open data with completed job codes at various locations in New York City to demonstrate some examples.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()


Initialize
----------
To be able to maintain provenance in openclean, a persistent instance of the openclean engine needs to be created. All versions of the
data files are maintained inside the base directory.

.. jupyter-execute::

    from openclean.engine.base import DB

    db = DB(basedir='./archive', create=True)


Create
------
The user can then register their dataset with the initialized openclean engine.

.. jupyter-execute::

    df = db.create(source=ds, name='jobs', primary_key='Job #')


The create method returns a pandas dataframe object which we shall apply some transformations on.


Commit
------
Let's create a new version of the dataset by performing lowercasing the Borough column.

.. jupyter-execute::

    from openclean.operator.transform.update import update

    df = db.checkout('jobs') # get the dataset

    lower_cased = update(df, columns='Borough', func=str.lower)

    lower_cased


The new dataset version can then be committed in the engine using the commit method.

.. jupyter-execute::

    db.commit(name='jobs', source=lower_cased)

To see the different versions of each dataset, we can simply request a log from the engine:

.. jupyter-execute::

    logs = db.dataset('jobs').log()
    logs


Checkout
--------
Users can checkout a previous version of a dataset to see what it looked like.

.. jupyter-execute::

    db.dataset('jobs').checkout(logs[0].version)


Rollback
--------
If the user is not happy with the changes, they can be rolled back to get the previous version of the dataset:

.. jupyter-execute::

    df = db.rollback('jobs', version=logs[0].version)

    df.head()


Register
--------
Additionally, the functionality is complemented by a GUI provided by :ref:`notebook-extension` that allows users to register
custom functions and apply it across datasets and versions seemlessly. A visual example of what this looks like
is present in the :ref:`custom-func-ref` section.


Other Examples
--------------
A full example notebook performing operations and maintaining provenance on a real dataset is available `here <https://github.com/VIDA-NYU/openclean-core/blob/dataset-history/examples/notebooks/engine/Openclean%20Engine%20-%20Datastore.ipynb>`_.