# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Profiling operator that computes data type statistics and raw type domain
assignments for columns in a data frame.
"""

from openclean.data.column import as_list, select_clause
from openclean.function.value.classifier import value_classifier
from openclean.function.value.datatype import is_int, is_float
from openclean.profiling.classifier.base import classprofile


"""Default value classifier that distinguishes between int, float, and text."""
DEFAULT_CLASSIFIER = value_classifier(
    classifier=[is_int(), is_float()],
    labels=['int', 'float'],
    default_label='text'
)


def datatypes(
    df, columns=None, classifier=None, picker=None, include_values=False
):
    """Compute list of raw data types and their counts for each (selected)
    column in a data frame. If the type picker is given, for each column a
    raw domain is selected and included in the result.

    The result is a list with one entry per column. Each entry is a dictionary
    that contains the column name ('name') and index position ('index'),
    statistics about type labels assigned for values in the column by the
    classifier ('types'). The list of selected types for the column domain
    ('domain') is only included if a type picker is given. If the
    include_values flag is True the list of distinct values together with their
    assigned clas labels is included in the result as well.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names. Allows
        to restrict the columns for which types are computed.
    classifier: openclean.function.classifier.base.ValueClassifier, optional
        Classifier that assigns data type class labels for scalar column
        values.
    picker: openclean.profiling.classifier.typepicker.TypePicker, optional
        Optional picker to select one (or more) raw type(s) as the domain for
        each column.
    include_values: bool, optional
        Include list of distinct column values and their assigned class
        labels in the result if the flag is True.

    Returns
    -------
    list
    """
    # Use the default classifier if no classifier is given.
    if classifier is None:
        classifier = DEFAULT_CLASSIFIER
    # Get name and index positions for all columns for which the raw data types
    # are being computed.
    if columns is not None:
        colnames, colidxs = select_clause(df, as_list(columns))
    else:
        colnames = as_list(df.columns)
        colidxs = list(range(len(colnames)))
    columns = zip(colnames, colidxs)
    # Compute data types and domains for all columns in the column list.
    result = list()
    for colname, colidx in columns:
        colinfo = dict({'name': colname, 'index': colidx})
        colprofile = classprofile(df, colidx, classifier)
        coltypes = colprofile['types']
        colinfo['types'] = coltypes
        if picker is not None:
            colinfo['domain'] = picker.select(coltypes)
        if include_values:
            colinfo['values'] = colprofile['values']
        result.append(colinfo)
    return result
