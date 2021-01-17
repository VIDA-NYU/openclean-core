Getting Started
===============


Loading Data
------------
openclean uses a dataset (a wrapped pandas dataframe) as it's primary data storage object.
It can be created from any source data type accepted by pandas. Compressed Gzip files (.gz) are also accepted.

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset('source/data/addresses.csv')

    ds.head()

for larger datasets, instead of loading the entire dataset into memory as above, openclean provides a streaming operator:

.. jupyter-execute::

    from openclean.pipeline import stream

    sm = stream('source/data/addresses.csv')

    print(sm)


Selecting
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

Two important ones that become building blocks for bigger operations are as follows.
The complete list of Eval Functions can be found in the `API Reference <index.html#api-ref>`_.

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


Inserting
---------
To insert a new column into a dataset, use the inscol operation. We use a Const eval function to define values for the
new 'City' column.

.. jupyter-execute::

    from openclean.operator.transform.insert import inscol
    from openclean.function.eval.base import Const

    new_col = inscol(ds, names=['City'], pos=4, values=Const('New York'))

    new_col.head()

To insert a new row, use the insrow operation. Let's add a dummy row at the start with all zeros.

.. jupyter-execute::

    from openclean.operator.transform.insert import insrow

    new_row = insrow(ds, pos=0, values=[0,0,0,0])

    new_row.head()

Updating
--------
Updating a preexisting column is straightforward. The update operator takes the column name and a func argument
which can be a callable or an Eval function. The following snippet updates the 'Borough' column to Title case.

.. jupyter-execute::

    from openclean.operator.transform.update import update

    title_case = update(ds, columns='Borough', func=str.title)

    title_case.head()

Filtering
---------
openclean filters records from a dataset using the filter operation, which requires a predicate. The predicate
is a list or dataframe of Booleans. Here, we use the Col eval function to create the predicate that translates to;
show all rows that have the value 'BROOKLYN' in the 'Borough' column.

.. jupyter-execute::

    from openclean.operator.transform.filter import filter
    from openclean.function.eval.base import Col

    filtered = filter(ds, predicate=Col('Borough')=='BROOKLYN')

    filtered.head()

Moving
------
Changing the column order is efficiently straight forward too. Let's move Job # to a different position.

.. jupyter-execute::

    from openclean.operator.transform.move import movecols

    moved_col = movecols(ds, 'Job #', 2)

    moved_col.head()

To move the an existing row to a different position, use the moverows operation. Here is an example:

.. jupyter-execute::

    from openclean.operator.transform.move import move_rows

    moved_row = move_rows(ds, 0, 2)

    moved_row.head()


Sorting
-------
To sort values in a column, openclean provides a sort operation. Let's try to sort the dataset in descending Job #s.

.. jupyter-execute::

    from openclean.operator.transform.sort import order_by

    sorted = order_by(ds, columns='Job #', reversed=True)

    sorted.head()


Cleaning Operators
==================
openclean comes with the following operators to help users wrangle their datasets, find anomalies in them and make fixes.

.. note:: This list is growing and will periodically be updated.

Missing data
------------


FDViolations
------------


Approx String Matching
----------------------


Custom functions
----------------
A user can create their own data cleaning operators, apply them and reuse them as per their requirements.
With :ref:`notebook-extension`, these eval functions or callables can further be registered on a UI and applied to
datasets visually. The following screen grab shows how custom functions together with
openclean-notebook enhance a user's data munging experience:

.. only:: html

   .. figure:: data/custom_func.gif


Profiling
=========
openclean comes with in built tools to profile datasets that help to report actionable metrics of the dataset. It
also provides a fairly easy to implement interface for users to create/attach their own data profilers as well.





Another :ref:`notebook-extension` feature is to be able to plot the profiled results and see them in the UI. The following
screen grab uses the `Auctus profiler <https://pypi.org/project/datamart-profiler/>`_ with the :ref:`notebook-extension` spreadsheet UI:

.. only:: html

   .. figure:: data/auctus_profiler.gif
