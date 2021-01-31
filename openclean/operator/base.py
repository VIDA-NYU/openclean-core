# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract classes for openclean pipeline operators. There are four primary
types of operators:

- DataFrameTransformer: The data frame transformer takes a DataFrame as input
  and generates a DataFrame as output.

- DataFrameMapper: The group generator takes as input a DataFrame and outputs
  a GroupBy.

- DataGroupReducer: The group reducer takes a  GroupBy as input and
  outputs a DataFrame.

- DataGroupTransformer: The group transformers takes aa GroupBy as
input and outputs a GroupBy.

In addition to the output DatFrame's or GroupBy object, each operator
can output a stage state object.
"""

from abc import ABCMeta, abstractmethod
from typing import List, Union

import pandas as pd

from openclean.data.groupby import DataFrameGrouping


"""Type alias definition for parameters and return values of different
operators in openclean.
"""
ColumnRef = Union[int, str]
Columns = Union[ColumnRef, List[Union[ColumnRef]]]
Names = Union[str, List[str]]


class PipelineStage(metaclass=ABCMeta):
    """Generic pipline stage interface."""
    def is_group_reducer(self) -> bool:
        """Test whether a pipeline operator is a (sub-)class of the data group
        reducer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataGroupReducer)

    def is_group_transformer(self) -> bool:
        """Test whether a pipeline operator is a (sub-)class of the data group
        transformer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataGroupTransformer)

    def is_frame_mapper(self) -> bool:
        """Test whether a pipeline operator is a (sub-)class of the data frame
        mapper type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameMapper)

    def is_frame_splitter(self) -> bool:
        """Test whether a pipeline operator is a (sub-)class of the data frame
        splitter type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameSplitter)

    def is_frame_transformer(self) -> bool:
        """Test whether a pipeline operator is a (sub-)class of the data frame
        transformer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameTransformer)


class DataGroupReducer(PipelineStage):
    """Abstract class for pipeline components that take a group of data frames
    as input and return a single data frame as output.
    """
    def __call__(self, groups: DataFrameGrouping) -> pd.DataFrame:
        """Make the operator callable. Executes the reduce method.

        Parameters
        ----------
        groups: openclean.data.groupby.DataFrameGrouping
            Grouping of pandas data frames.

        Returns
        -------
        pd.DataFrame
        """
        return self.reduce(groups)

    @abstractmethod
    def reduce(self, groups: DataFrameGrouping) -> pd.DataFrame:
        """This is the main method that each subclass of the group reducer has
        to implement. The input is a pandas data frame grouping. The output is
        a single data frame.

        Parameters
        ----------
        groups: openclean.data.groupby.DataFrameGrouping
            Grouping of pandas data frames.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()


class DataGroupTransformer(PipelineStage):
    """Abstract class for pipeline components that take a data frame grouping
    as input and return a transformed grouping.
    """
    def __call__(self, df):
        """Make the operator callable. Executes the transform method.

        Parameters
        ----------
        groups: openclean.data.groupby.DataFrameGrouping
            Grouping of pandas data frames.

        Returns
        -------
        openclean.data.groupby.DataFrameGrouping
        """
        return self.transform(df)

    @abstractmethod
    def transform(self, groups):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame grouping. The output is a
        modified data frame grouping.

        Parameters
        ----------
        groups: openclean.data.groupby.DataFrameGrouping
            Grouping of pandas data frames.

        Returns
        -------
        openclean.data.groupby.DataFrameGrouping
        """
        raise NotImplementedError()


class DataFrameMapper(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and return a data frame grouping as output.
    """
    def __call__(self, df):
        """Make the operator callable. Executes the map method.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        openclean.data.groupby.DataFrameGrouping
        """
        return self.map(df)

    @abstractmethod
    def map(self, df):
        """This is the main method that each subclass of the mapper has to
        implement. The input is a pandas data frame. The output is a group of
        pandas data frames.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        openclean.data.groupby.DataFrameGrouping
        """
        raise NotImplementedError()


class DataFrameSplitter(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and returns two data frames as output. This is a special case of the data
    frame mapper.
    """
    def __call__(self, df):
        """Make the operator callable. Executes the split method.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame, pd.DataFrame
        """
        return self.split(df)

    @abstractmethod
    def split(self, df):
        """This is the main method that each subclass of the splitter has to
        implement. The input is a pandas data frame. The output are two data
        frames.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame, pd.DataFrame
        """
        raise NotImplementedError()


class DataFrameTransformer(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and return a transformed data frame as output.
    """
    def __call__(self, df):
        """Make the operator callable. Executes the transform method.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame
        """
        return self.transform(df)

    @abstractmethod
    def transform(self, df):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame. The output is a modified
        data frame.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()
