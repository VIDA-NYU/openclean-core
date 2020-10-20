# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Counter for profiling purposes. The Count class allows to evaluate a
predicate on all rows in a data frame and count the number of rows that
satisfy the predicate.
"""

from typing import Dict, Optional

import pandas as pd

from openclean.data.stream.df import DataFrameStream
from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction
from openclean.pipeline.consumer.collector import RowCount
from openclean.pipeline.processor.collector import CollectOperator
from openclean.pipeline.processor.producer import FilterOperator


def Count(
    predicate=EvalFunction, truth_value: Optional[Scalar] = True
) -> FilterOperator:
    """Wrapper for predicates and their truth value. Returns a filter operator
    that can then be used to count the number of rows that satisfy the
    predicate.

    Parameters
    ----------
    predicate: openclean.function.eval.base.EvalFunction
        Evaluation function that is used as the predicate for filtering
        data stream rows. The function will be prepared in the
        create_consumer method.
    truth_value: scalar, defaut=True
        Return value of the predicate that signals that the predicate is
        satisfied by an input value.

    Returns
    -------
    openclean.pipeline.processor.producer.FilterOperator
    """
    return FilterOperator(predicate=predicate, truth_value=truth_value)


def counts(df: pd.DataFrame, counters: Dict[str, FilterOperator]) -> Dict:
    """Evaluate a set of counters (predicates) on a given data frame. Expects
    a mapping of elements to strema filter that evaluate a predicate. Returns
    the number of filtered rows for each filter in the resulting dictionary.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame that is being profiled.
    counters: dict
        Dictionary mapping unique keys to filter operators that evaluate a
        predicate on the rows of the data frame.

    Returns
    -------
    dict
    """
    ds = DataFrameStream(df)
    consumers = dict()
    for key, op in counters.items():
        if isinstance(op, EvalFunction):
            op = Count(op)
        consumers[key] = op.open(
            ds=ds,
            schema=ds.columns,
            downstream=[CollectOperator(RowCount)]
        )
    for rid, values in ds.iterrows():
        for consumer in consumers.values():
            consumer.consume(rowid=rid, row=values)
    result = dict()
    for key, consumer in consumers.items():
        result[key] = consumer.close()
    return result
