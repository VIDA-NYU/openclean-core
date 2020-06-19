# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions that implement delete operations for columns and rows. The
operators in this module operate on lists of identifier for columns and rows.
They are motivated by the Vizier VizUAL language."""

from openclean.operator.transform.select import Select


def delete_columns(df, colids):
    """Delete the columns with the given identifier from the schema of the
    data frame. Raises a ValueError if the column identifier list contains
    values that do not reference columns in the data frame schema.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    colids: int or list(int)
        Single column identifier or list of column indentifier.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Use a lookup for identifier of columns that are to be deleted
    delcols = set(colids) if isinstance(colids, list) else set([colids])
    # Implemnted using the select operator. For this, we need to get the index
    # positions for thoses columns that are not being deleted.
    select_clause = list()
    for colidx, col in enumerate(df.columns):
        try:
            # Attempt to access the column id. If the identifier is for a
            # column that is to be deleted remove it from the delcols list.
            # Otherwise, add the index position of the column to the select
            # clause.
            if col.colid in delcols:
                delcols.remove(col.colid)
            else:
                select_clause.append(colidx)
        except AttributeError:
            select_clause.append(colidx)
    # Raise an error if not all of the identifier in delcols have been removed.
    if delcols:
        raise ValueError("unknown column identifier {}".format(list(delcols)))
    return Select(columns=select_clause).transform(df)


def delete_rows(df, rowids):
    """Delete the rows with the given identifier from the data frame. Raises a
    ValueError if the row identifier list contains values that do not reference
    rows in the data frame index.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    rowids: int or list(int)
        Single row identifier or list of row indentifier.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Use lookup for rows that are deleted
    delrows = set(rowids) if isinstance(rowids, list) else set([rowids])
    # Create a Boolean array to maintain information about the rows that
    # satisfy are being kept.
    smap = [False] * len(df.index)
    index = 0
    for rowid, values in df.iterrows():
        if rowid in delrows:
            delrows.remove(rowid)
        else:
            smap[index] = True
        index += 1
    # Raise an error if not all of the identifier in delrows have been removed.
    if delrows:
        raise ValueError("unknown row identifier {}".format(list(delrows)))
    # Return data frame containing those rows whose identifier were not in the
    # delete list.
    return df[smap]
