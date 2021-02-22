.. _concepts-ref:

Data Model
==========

openclean provides useful functionality to identify bugs and anomalous values, make fixes and wrangle datasets. To help
illustrate different operations, we use a sample of NYC open data with completed job codes at various locations in New York City.

Datasets and Streams
--------------------
openclean uses a dataset (a wrapped pandas dataframe) as it's primary data storage object.
It can be created from any source data type accepted by pandas. Compressed Gzip files (.gz) are also accepted.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()

For larger datasets, instead of loading the entire dataset into memory as above, openclean provides a streaming operator:

.. jupyter-execute::

    from openclean.pipeline import stream

    sm = stream(os.path.join(path_to_file, 'job_locations.csv'))

    print(sm)

A dataset stream can directly have operators applied to it and the resulting dataset can computed lazily. We provide
a `jupyter notebook <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/city-names/DOB%20Job%20Application%20Filings%20-%20Brooklyn%20Spellings.ipynb>`_ to demonstrate the streaming functionality further.

Eval Functions
--------------
Evaluation functions are used to compute results over rows in a dataset
or a data stream. Conceptually, evaluation functions are evaluated
over one or more columns for each row in the input data. For each row, the
function is expected to generate one (or more) (transformed) value(s) for
the column (columns) on which it operates.

Evaluation functions are building blocks for data frame operators as well
as data stream pipelines. Each of these two use cases is supported by a
different (abstract) method:

    * eval: The eval function is used by data frame operators. The function
      receives the full data frame as an argument. It returns a data series
      (or list) of values with one value for each row in the input data frame.
      Functions that operate over multiple columns will return a list of
      tuples.

    * prepare: If an evaluation function is used as part of a data stream
      operator the function needs to be prepared. That is, the function will
      need to know the schema of the rows in the data frame before streaming
      starts. The prepare method receives the schema of the data stream as an
      argument. It returns a callable function that accepts a data stream row
      as the only argument and that returns a single value or a tuple of values
      depending on whether the evaluation function operators on one or more
      columns.

Evaluation functions can be considered as wrappers around callables that store column
information and can be passed around through openclean pipelines. The eval and prepare methods execute them.
Here is a basic Eval function:

.. jupyter-execute::

    from openclean.function.eval.base import Eval

    lower_case = Eval('Borough', str.lower)

    print(ds['Borough'].to_list())
    print()
    print(lower_case.eval(ds))


Some important Eval functions that become building blocks for bigger operations are as follows.
The whole range of eval functions and value functions can be accessed in the `Eval package of the API Reference <api/openclean.function.eval.html>`_
and `Value package of the API Reference <api/openclean.function.value.html>`_ respectively.

Col
^^^
Col is an Evaluation function that returns the value from a single column in a data frame.

.. jupyter-execute::

    from openclean.function.eval.base import Col

    boro = Col('Borough').eval(ds)

    print(boro)

Cols
^^^^
Cols is an Evaluation function that returns the values from a multiple columns in a data frame row. Let's try
to get values from 2 columns together. Multiple columns are returned as a list of tuples:

.. jupyter-execute::

    from openclean.function.eval.base import Cols

    job_locations = Cols(['Job #','Borough']).eval(ds)

    print(job_locations)


Const
^^^^^
Const is an Evaluation function that creates a column with the provided constant value. For e.g:

.. jupyter-execute::

    from openclean.function.eval.base import Const

    complaint_phone = Const('311').eval(ds)

    print(complaint_phone)


And
^^^
And is an important logical Evaluation function that validates whether the outputs of the input functions are all true and creates a list of predicates.

.. jupyter-execute::

    from openclean.function.eval.logic import And

    pred = And(Eval('Borough', str.lower) == str.lower('BROOKLYN'), Col('Street Name') == 'BROADWAY').eval(ds)

    print(ds[pred])

Or
^^
Or validates that at least one of the outputs of the input functions is true and creates a list of predicates.

.. jupyter-execute::

    from openclean.function.eval.logic import Or

    pred = Or(Eval('Borough', str.lower) == str.lower('BROOKLYN'), Col('Street Name') == 'BROADWAY').eval(ds)

    print(ds[pred])
