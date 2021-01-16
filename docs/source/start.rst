Getting Started
===============


Load Data
---------
openclean uses a dataset (a wrapped pandas dataframe) as it's primary data storage object.
It can be created from any source data type accepted by pandas. Compressed Gzip files (.gz) are also accepted.

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset('source/data/addresses.csv')

    ds.head()

for larger datasets, instead of loading the entire dataset into memory as above,
openclean provides a streaming operator:

.. jupyter-execute::

    from openclean.pipeline import stream

    sm = stream('source/data/addresses.csv')

    print(sm)


Selection
---------
One can select columns from a dataset using the select operation:

.. jupyter-execute::

    from openclean.operator.transform.select import select

    selected = select(ds, columns=['Job #', 'GIS_NTA_NAME'], names=['job_id','neighborhood'])

    selected.head()


Eval Functions
--------------
Evaluation functions are used to compute results over rows in a dataframe
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

Some important ones that become building blocks for bigger operations are as follows.
The complete list of Eval Functions can be found in the **API reference section**.

Col
^^^
Col is an Evaluation function that returns the value from a single column in a
data frame row.

.. jupyter-execute::

    from openclean.function.eval.base import Col

    boro = Col('Borough').eval(ds)

    print(boro)

Cols
^^^^
Cols is an Evaluation function that returns the values from a multiple columns in a
data frame row.

.. jupyter-execute::

    from openclean.function.eval.base import Cols

    job_locations = Cols(['Job #','Borough']).eval(ds)

    print(job_locations)


Filter
------
openclean selects records from a dataset using the filter operation, which requires a predicate. The predicate
is an Eval Function:

.. jupyter-execute::

    from openclean.operator.transform.filter import filter
    from openclean.function.eval.base import Col

    filtered = filter(ds, predicate=Col('Borough')=='BROOKLYN')

    filtered.head()


Missing data
Operations
Merge
Grouping
Reshaping
Time series
Categoricals
Plotting
Getting data in/out
Gotchas
Insert
Delete
Viewing data


Cleaning Operators
------------------
FDViolations
Approx String Matching
Custom functions

Profiling
---------

