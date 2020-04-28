"""Abstract classes for pipeline operators. There are four main operator types:

- DataFrameTransformer: The data frame transformer takes a DataFrame as input
  and generates a DataFrame as output.

- DataFrameMapper: The group generator takes as input a DataFrame and outputs
  a DataFrameGroupBy.

- DataGroupReducer: The group reducer takes a  DataFrameGroupBy as input and
  outputs a DataFrame.

- DataGroupTransformer: The group transformers takes aa DataFrameGroupBy as
input and outputs a DataFrameGroupBy.

In addition to the output DatFrame's or DataFrameGroupBy object, each operator
can output a stage state object.
"""

from abc import ABCMeta, abstractmethod


class PipelineStage(metaclass=ABCMeta):
    """Generic pipline stage interface."""
    def is_group_reducer(self):
        """Test whether a pipeline operator is a (sub-)class of the data group
        reducer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataGroupReducer)

    def is_group_transformer(self):
        """Test whether a pipeline operator is a (sub-)class of the data group
        transformer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataGroupTransformer)

    def is_frame_mapper(self):
        """Test whether a pipeline operator is a (sub-)class of the data frame
        mapper type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameMapper)

    def is_frame_splitter(self):
        """Test whether a pipeline operator is a (sub-)class of the data frame
        splitter type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameSplitter)

    def is_frame_transformer(self):
        """Test whether a pipeline operator is a (sub-)class of the data frame
        transformer type.

        Returns
        -------
        bool
        """
        return isinstance(self, DataFrameTransformer)


class DataGroupReducer(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and return a transformed data frame as output. In addition to transforming
    the data frame, the operator can add to the global state of the pipeline.
    """
    def __init__(self, stage_label=None):
        """Initialize the stage label in the super class.

        Parameters
        ----------
        stage_label: string, optional
            User-provided stage label.
        """
        super(DataGroupReducer, self).__init__(stage_label=stage_label)

    def __call__(self, df, state):
        """Make the operator callable. Executes the apply method.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        return self.apply(df, state)

    @abstractmethod
    def apply(self, df, state):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame and the current global
        pipeline state. The output is a modified data frame and a optional
        stage state object (dictionary).

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        raise NotImplementedError()


class DataGroupTransformer(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and return a transformed data frame as output. In addition to transforming
    the data frame, the operator can add to the global state of the pipeline.
    """
    def __init__(self, stage_label=None):
        """Initialize the stage label in the super class.

        Parameters
        ----------
        stage_label: string, optional
            User-provided stage label.
        """
        super(DataGroupTransformer, self).__init__(stage_label=stage_label)

    def __call__(self, df, state):
        """Make the operator callable. Executes the apply method.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        return self.apply(df, state)

    @abstractmethod
    def apply(self, df, state):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame and the current global
        pipeline state. The output is a modified data frame and a optional
        stage state object (dictionary).

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        raise NotImplementedError()


class DataFrameMapper(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and return a transformed data frame as output. In addition to transforming
    the data frame, the operator can add to the global state of the pipeline.
    """
    def __init__(self, stage_label=None):
        """Initialize the stage label in the super class.

        Parameters
        ----------
        stage_label: string, optional
            User-provided stage label.
        """
        super(DataFrameMapper, self).__init__(stage_label=stage_label)

    def __call__(self, df, state):
        """Make the operator callable. Executes the apply method.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        return self.apply(df, state)

    @abstractmethod
    def apply(self, df, state):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame and the current global
        pipeline state. The output is a modified data frame and a optional
        stage state object (dictionary).

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        state: openclean.pipeline.state.GlobalState
            Global pipeline state object

        Returns
        -------
        (pandas.DataFrame, dict)
        """
        raise NotImplementedError()


class DataFrameSplitter(PipelineStage):
    """Abstract class for pipeline components that take a data frame as input
    and returns two data frames as output.
    """
    def __call__(self, df):
        """Make the operator callable. Executes the transform method.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        return self.transform(df)

    @abstractmethod
    def split(self, df):
        """This is the main method that each subclass of the splitter has to
        implement. The input is a pandas data frame. The output are two data
        frames.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame, pandas.DataFrame
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
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        return self.transform(df)

    @abstractmethod
    def transform(self, df):
        """This is the main method that each subclass of the transformer has to
        implement. The input is a pandas data frame. The output is a modified
        data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        raise NotImplementedError()
