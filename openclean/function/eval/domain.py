# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test for containment of column values in value sets."""

from typing import Dict, List, Optional, Union

import pandas as pd

from openclean.data.stream.base import DataRow, StreamFunction
from openclean.data.types import DatasetSchema, Value
from openclean.data.util import to_set
from openclean.function.eval.base import InputColumn, Eval, EvalFunction, EvalResult
from openclean.function.eval.base import evaluate, to_eval
from openclean.function.value.domain import IsInDomain, IsNotInDomain


# -- Domain membership predicates ---------------------------------------------

class IsIn(Eval):
    """Boolean predicate to tests whether a value (or list of values) belong(s)
    to a domain of known values.
    """
    def __init__(self, columns, domain, ignore_case=False):
        """Create an instance of an evaluation function that checks for domain
        inclusion.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, default=False
            Ignore case for domain inclusion checking
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        super(IsIn, self).__init__(
            func=IsInDomain(domain, ignore_case=ignore_case),
            columns=columns,
            is_unary=True
        )


class IsNotIn(Eval):
    """Boolean predicate that tests whether a value (or list of values) dos not
    belong to a domain of knwon values.
    """
    def __init__(self, columns, domain, ignore_case=False):
        """Create an instance of an evaluation function that checks for domain
        exclusion.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, default=False
            Ignore case for domain inclusion checking
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        super(IsNotIn, self).__init__(
            func=IsNotInDomain(domain, ignore_case=ignore_case),
            columns=columns,
            is_unary=True
        )


# -- Lookup tables ------------------------------------------------------------

class Lookup(EvalFunction):
    """A Lookup table is a mapping function. For a given lookup value the
    result is the mapped value from a given dictionary if a mapping exists.
    Otherwise, the returned value is generated from a default value function.
    If the default value function is not defined then the input value is
    returned as the result.

    The aim of having default as a evaluation function is to enable lookups of
    values in one column using an incomplete lookup table but updating the
    values a separate column (other than the lookup column). In this case, the
    lookup value is not the default value.
    """
    def __init__(
        self, columns: Union[InputColumn, List[InputColumn]], mapping: Dict,
        default: Optional[Union[InputColumn, List[InputColumn]]] = None
    ):
        """Initialize the mapping and the default value function.

        Parameters
        ----------
        columns: list, tuple, or openclean.function.eval.base.EvalFunction
            Evaluation function to extract values from data frame rows. This
            can also be a list or tuple of evaluation functions or a list of
            column names or index positions.
        mapping: dict
            Mapping from input to output values. The type of the keys in the
            mapping dictionary are expected to match the value type that is
            defined by the columns list.
        default:  list, tuple, or openclean.function.eval.base.EvalFunction,
                default=None
            Evaluation function (or list of evaluation functions) to generate
            the mapping result for input values that are not in the given
            lookup table.
        """
        # Generate a list of producers. Producers are evaluation functions that
        # generate the input values for the consumer.
        self.producers = to_eval(columns)
        # Keep the mapping as is.
        self.mapping = mapping
        # Ensure that default is a list if given.
        self.default = to_eval(default) if default is not None else None

    def eval(self, df: pd.DataFrame) -> EvalResult:
        """Evaluate the consumer on the lists of values that are generated by
        the referenced columns.

        Parameters
        ----------
        df: pd.DataFrame
            Pandas data frame.

        Returns
        -------
        pd.Series or list
        """
        # Start by getting the lookup values from the producers.
        values = evaluate(df=df, producers=self.producers)
        # Generate default values (if default producer(s) are given). If no
        # default was specified the lookup values are used as defaults.
        if self.default is not None:
            defaults = evaluate(df=df, producers=self.default)
        else:
            defaults = values

        # unpack user set indices to allow to iterate through an IndexRange
        if isinstance(defaults, pd.Series):
            defaults.reset_index(drop=True, inplace=True)

        # Generate a list with a lookup result per data frame row.
        data = list()
        for value in values:
            data.append(self.mapping.get(value, defaults[len(data)]))
        return data

    def prepare(self, columns: DatasetSchema) -> StreamFunction:
        """Prepare the evaluation function to be able to process rows in a data
        stream. This method is called before streaming starts to inform the
        function about the schema of the rows in the data stream.

        Prepare is expected to return a callable that accepts a single data
        stream row as input and that returns a single value (if the function
        operates on a single column) or a tuple of values (for functions that
        operate over multiple columns).

        Parameters
        ----------
        columns: list of string
            List of column names in the schema of the data stream.

        Returns
        -------
        openclean.data.stream.base.StreamFunction
        """

        # Prepare the producer and optional defaults.
        producers = [f.prepare(columns) for f in self.producers]
        defaults = [f.prepare(columns) for f in self.default] if self.default else None
        # mapping = self.mapping

        # Create a lookup stream function.

        def lookup(row: DataRow) -> Value:
            # Generate values from the row.
            if len(producers) == 1:
                value = producers[0](row)
            else:
                value = tuple([f(row) for f in producers])
            if value in self.mapping:
                return self.mapping[value]
            elif defaults is not None:
                if len(defaults) == 1:
                    return defaults[0](row)
                else:
                    return tuple([f(row) for f in defaults])
            return value

        return lookup
