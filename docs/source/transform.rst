.. _transform-ref:

Data Transformation
===================
openclean provides it's own set of operations to transform datasets whilst keeping row indices intact.

We use a sample of NYC open data with completed job codes at various locations in New York City to demonstrate some examples.

.. jupyter-execute::

    import os
    path_to_file = os.path.join(os.getcwd(), 'source', 'data')

.. jupyter-execute::

    from openclean.data.load import dataset

    ds = dataset(os.path.join(path_to_file, 'job_locations.csv'))

    ds.head()


Selecting
---------
One can select columns from a dataset using the select operation:

.. jupyter-execute::

    from openclean.operator.transform.select import select

    selected = select(ds, columns=['Job #', 'GIS_NTA_NAME'], names=['job_id','neighborhood'])

    selected.head()


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
which can be a callable or an Eval function. The following snippet updates the 'Borough' column to Title case. The
func can be a dictionary, a scalar or a function.

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

