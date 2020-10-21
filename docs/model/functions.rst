=========================================
Different Types of Functions in openclean
=========================================

In openclean we distinguish between **functions** that operate on the data in a dataset and **operators** that manipulate datasets. The main distinction is that **functions** return values or data series whereas **operators** return datasets or groups of datasets. Thus, operators are the building blocks for data cleaning and transformation pipelines while functions are the building blocks for the operators.


Evaluation Functions
====================

Evaluation functions represent statements (expressions) that are evaluated over one or more columns in a data frame. The evaluation function will always receive the full data frame and it is expected to return a data series or an array with a length that matches the number of rows in the data frame. The result may contain one or more columns.

The evaluation function implements the eval(df: pd.DataFrame) -> EvalResult function, where eval result is list or pd.DataSeries.

Evaluation functions can be used in data streams. For stream the function needs to be prepared in that if the function accesses (extracts) data from a column/columns of the data stream it has to knwo the schema. Thus, the prepare methodreturns a callable that accepst a data stream row.

Conventions: an evaluation function the expects a single column should have an argument `column` of type `ColumnRef`. A evaluation function that accepts one or more columns should have a parameter `columns: ColumnsRef`.


Value Functions
===============

Value functions operate on one or more (scalar) values and return a single value or a tuple of values. The abstract class :class:`ValueFunction` defines three methods: `eval`, `is_prepared` and `prepare`. Value functions are normally wrapped into an evaluation function. The evaluation function is responsible for extracting values from datasets that are then passed to the value function for evaluation. Depending on whether the evaluation function operates on a single column or multiple columns the wrapped value function will either receive a single scalar value as the ergument for the eval function or a tuple of scalar values. It is the responsibility of the user that defines the operations in a data cleaning workflow to ensure that the expected argument type of a value function matches the value type that the evaluation function will extract and pass to the function (same for the results that are returned by a value function).
