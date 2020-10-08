# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Consumer for data frame rows in a stream environment."""

from abc import ABCMeta, abstractmethod
from collections import Counter
from typing import Any, List, Optional, Union

import pandas as pd

from openclean.data.column import Column
from openclean.data.load.csv import CSVWriter
from openclean.data.select import select_clause
from openclean.function.eval.base import EvalFunction


# -- Abstract base class for data stream consumers ----------------------------

class StreamConsumer(metaclass=ABCMeta):
    def __init__(self, consumer=None):
        """Initialize the downstream consumer that handles the processed values
        from this consumer.

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer, default=None
            Consumer that handles the processed rows from this consumer.
        """
        self.consumer = consumer

    @abstractmethod
    def close(self) -> Any:
        """Signal that the end of the data stream has reached. The return value
        is implementation dependent.

        Returns
        -------
        any
        """
        raise NotImplementedError()

    def consume(self, rowid: int, row: List) -> List:
        """Consume the given row. Calls the handle method to process the row.
        Only if the returned value from the handle() method is not None will
        the processed result be passed on to the downstream consumer.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        values = self.handle(rowid, row)
        if values is not None:
            if self.consumer is not None:
                return self.consumer.consume(rowid, values)
        return values

    @abstractmethod
    def handle(self, rowid: int, row: List) -> List:
        """Process a given row. Return a modified row or None. In the latter
        case it is assumed that the row should not be passed on to any consumer
        downstream.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        raise NotImplementedError()

    @abstractmethod
    def open(self, schema: List[Union[str, Column]]):
        """Signal that the consumer is about to receive the first row in the
        data stream. Contains the list of column names that define the schema
        of the rows in the stream.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        raise NotImplementedError()

    @abstractmethod
    def set_consumer(self, consumer):
        """Return a new instance of this class where the given consumer is set
        as the final consumer (i.e., the sink of the data stream).

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.

        Returns
        -------
        openclean.data.load.consumer.StreamConsumer
        """
        raise NotImplementedError()


# -- Data stream consumers ----------------------------------------------------

class DataFrame(StreamConsumer):
    """Row collector that generates a pandas data frame from the rows in a
    data stream. This consumer will not accept a downstream consumer as it
    would never send any rows to such a consumer.
    """
    def __init__(self):
        """Initialize empty lists for data frame columns, rows and the row
        identifier. These lists will be initialized when the consumer receives
        the open signal.
        """
        self.columns = None
        self.data = None
        self.index = None

    def close(self) -> pd.DataFrame:
        """Closing the consumer yields the data frame with the collected rows.

        Returns
        -------
        ps.DataFrame
        """
        return pd.DataFrame(
            data=self.data,
            columns=self.columns,
            index=self.index
        )

    def handle(self, rowid: int, row: List):
        """Add the row identifier and row values to the respective lists.
        Returns None to avoid that the (empty) downstream consumer is called.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.data.append(row)
        self.index.append(rowid)

    def open(self, schema):
        """Initialize the different lists that collect the different components
        of the generated data frame.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        self.columns = schema
        self.data = list()
        self.index = list()

    def set_consumer(self, consumer: StreamConsumer):
        """The data frame collector does not accept any downstream consumers.

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.
        """
        raise NotImplementedError()


class Distinct(StreamConsumer):
    """Consumer that popuates a counter with the frequency counts for distinct
    values (value combinations) in the processed rows for the data stream.
    """
    def __init__(self):
        """Define the counter object. The object will be initialized during the
        open() call.
        """
        self.counter = None

    def close(self) -> Counter:
        """Closing the consumer yields the populated Counter object.

        Returns
        -------
        collections.Counter
        """
        return self.counter

    def handle(self, rowid: int, row: List):
        """Add the value combination for a given row to the counter. If the
        row contains more than one column a tuple of column values will be
        added to the counter.

        Returns None to avoid that the (empty) downstream consumer is called.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        if len(row) == 1:
            self.counter[row[0]] += 1
        else:
            self.counter[tuple(row)] += 1

    def open(self, schema):
        """Initialize the value counter.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        self.counter = Counter()

    def set_consumer(self, consumer: StreamConsumer):
        """The distint value counter does not accept any downstream consumers.

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.
        """
        raise NotImplementedError()


class Filter(StreamConsumer):
    """Filter for rows in a data stream."""
    def __init__(
        self, predicate: EvalFunction,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the predicate that is used to filer rows in the stream.
        The predicated is expected to be a evaluation function that will be
        prepared in the open() call.

        Parameters
        ----------
        predicate: openclean.function.eval.base.EvalFunction
            Evaluation function that is used as the predicate to filter rows
            in the data stream.
        consumer: openclean.data.load.consumer.StreamConsumer, default=None
            Optional downstream consumer.
        """
        super(Filter, self).__init__(consumer=consumer)
        self.predicate = predicate
        self.prepared_predicate = None
        # Maintain the list of columns in the data stream schema for the
        # prepare call of the predicate.
        self.columns = None

    def close(self) -> Any:
        """Return the result of the associated consumer when the end of the
        data stream was reached.

        Returns
        -------
        any
        """
        if self.consumer is not None:
            return self.consumer.close()

    def handle(self, rowid: int, row: List) -> List:
        """Evaluate the predicate on the given row. Only if the predicate is
        satisfied will the row be passed on to the downstream consumer.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        if self.prepared_predicate.eval(row):
            return row

    def open(self, schema):
        """Prepare the associated predicate. The predicate requires the schema
        information for the data stream. The schema is set in the local columns
        property and the consumer is passed to the predicate instead of a data
        frame for preparation.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        # Set the data stream schema. This will be accessed when passing the
        # consumer as an argument to the prepare call of the associated
        # predicate.
        self.columns = schema
        # Prepare the predicate for the data stream schema.
        self.prepared_predicate = self.predicate.prepare(self)
        # Open the associated downstream consumer.
        if self.consumer is not None:
            self.consumer.open(schema)

    def set_consumer(self, consumer: StreamConsumer) -> StreamConsumer:
        """Return a new instance of Filter where the given consumer is set
        as the final consumer (i.e., the sink of the data stream).

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.

        Returns
        -------
        openclean.data.load.consumer.Filter
        """
        if self.consumer is not None:
            return Filter(
                predicate=self.predicate,
                consumer=self.consumer.set_consumer(consumer)
            )
        else:
            return Filter(predicate=self.predicate, consumer=consumer)


class Limit(StreamConsumer):
    def __init__(self, limit: int, consumer: Optional[StreamConsumer] = None):
        """Initialize the row limit and the optional downstream consumer.

        Parameters
        ----------
        limit: int
            Maximum number of rows that are passed on to the downstream
            consumer.
        consumer: openclean.data.load.consumer.StreamConsumer, default=None
            Optional downstream consumer.
        """
        super(Limit, self).__init__(consumer=consumer)
        self.limit = limit
        self.count = None

    def close(self) -> Any:
        """Return the result of the associated consumer when the end of the
        data stream was reached.

        Returns
        -------
        any
        """
        if self.consumer is not None:
            return self.consumer.close()

    def handle(self, rowid: int, row: List) -> List:
        """Pass the row on to the downstream consumer if the row limit has not
        been reached yet. Otherwise, a StopIteration error is raised.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        if self.count < self.limit:
            self.count += 1
            return row
        else:
            raise StopIteration()

    def open(self, schema):
        """Reset the row counter at the beginning of the stream.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        self.count = 0
        if self.consumer is not None:
            self.consumer.open(schema)

    def set_consumer(self, consumer: StreamConsumer) -> StreamConsumer:
        """Return a new instance of Limit where the given consumer is set
        as the final consumer (i.e., the sink of the data stream).

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.

        Returns
        -------
        openclean.data.load.consumer.Limit
        """
        if self.consumer is not None:
            return Limit(
                limit=self.limit,
                consumer=self.consumer.set_consumer(consumer)
            )
        else:
            return Limit(limit=self.limit, consumer=consumer)


class Select(StreamConsumer):
    def __init__(
        self, columns: List[Union[str, Column]],
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the list of columns that are included in the output
        data stream rows from this consumer and the optional downstream
        consumer.

        Parameters
        ----------
        columns: list of string
            List of column names for the resulting data stream schema of this
            consumer.
        consumer: openclean.data.load.consumer.StreamConsumer, default=None
            Optional downstream consumer.
        """
        super(Select, self).__init__(consumer=consumer)
        self.columns = columns
        self.colidxs = None

    def close(self) -> Any:
        """Return the result of the associated consumer when the end of the
        data stream was reached.

        Returns
        -------
        any
        """
        if self.consumer is not None:
            return self.consumer.close()

    def handle(self, rowid: int, row: List) -> List:
        """Return a row that only contains the columns in the selected list.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        return [row[i] for i in self.colidxs]

    def open(self, schema):
        """Initialize the names and index positionsof the selected columns with
        respect to the schema of the data stream rows that will be passed on to
        this consumer.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        colnames, self.colidxs = select_clause(schema, self.columns)
        if self.consumer is not None:
            self.consumer.open(colnames)

    def set_consumer(self, consumer: StreamConsumer) -> StreamConsumer:
        """Return a new instance of Filter where the given consumer is set
        as the final consumer (i.e., the sink of the data stream).

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.

        Returns
        -------
        openclean.data.load.consumer.Select
        """
        if self.consumer is not None:
            return Select(
                columns=self.columns,
                consumer=self.consumer.set_consumer(consumer)
            )
        else:
            return Select(columns=self.columns, consumer=consumer)


class Write(StreamConsumer):
    def __init__(self, writer: CSVWriter):
        """Initialize the CSV writer.

        Parameters
        ----------
        writer: openclean.data.load.csv.CSVWriter
            Writer for rows in a CSV file.
        """
        self.writer = writer

    def close(self):
        """Close the associated CSV writer when the end of the data stream was
        reached.
        """
        self.writer.close()

    def handle(self, rowid: int, row: List):
        """Write the row values to the output file.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.writer.write(row)

    def open(self, schema):
        """Write the columns in the data stream schema to the output file.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        self.writer.write(schema)

    def set_consumer(self, consumer: StreamConsumer):
        """The writer does not accept any downstream consumers.

        Parameters
        ----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer that handles the final processed rows in a data stream.
        """
        raise NotImplementedError()
