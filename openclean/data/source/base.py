# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base classes that provide access to datasets that are available via
public opendata repositories.
"""

from abc import ABCMeta, abstractmethod
from typing import IO, Iterable, List, Optional
from openclean.data.base import Descriptor

import pandas as pd


class ColumnDescriptor(Descriptor):
    """Descriptor for a dataset column. Extends the data object descriptor with
    a data type field.
    """
    def __init__(
        self, identifier: str, dtype: str, name: Optional[str] = None,
        description: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique object identifier.
        dtype: string
            Data type identifier. We currently do not define a controlled
            vocabulary for possible values.
        name: string, default=None
            Human-readable name.
        description: string, default=None
            Descriptive help text.
        """
        super(ColumnDescriptor, self).__init__(
            identifier=identifier,
            name=name,
            description=description
        )
        self.dtype = dtype


class DatasetHandle(Descriptor, metaclass=ABCMeta):
    """The dataset handle extends the dataset descriptor with a list of column
    descriptors for the columns in the dataset schema. The handle also defines
    abstract methods that provide access to the dataset as a data frame or to
    write the dataset to file.
    """
    def __init__(
        self, identifier: str, columns: List[ColumnDescriptor],
        name: Optional[str] = None, description: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique object identifier.
        columns: list of openclean.data.base.ColumnDescriptor
            Descriptors for the columns in the dataset schema.
        name: string, default=None
            Human-readable name.
        description: string, default=None
            Descriptive help text.
        """
        super(DatasetHandle, self).__init__(
            identifier=identifier,
            name=name,
            description=description
        )
        self.columns = columns

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """Access the dataset as a pandas data frame. This may download the
        dataset and convert it into a data frame.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def write(self, file: IO):
        """Write the dataset to the given file. The output file format is
        depending on the data source (or the dataset itself). The format should
        be a csv format where possible.

        Parameters
        ----------
        file: file object
            File-like object that provides a write method.
        """
        raise NotImplementedError()  # pragma: no cover


class DataRepository(Descriptor, metaclass=ABCMeta):
    """Handle for a repository that provides access to open datasets."""
    def __init__(
        self, identifier: str, name: Optional[str] = None,
        description: Optional[str] = None
    ):
        """Initialize the data repository descriptor.

        Parameters
        ----------
        identifier: string
            Unique  repository identifier.
        name: string, default=None
            Human-readable repository name.
        description: string, default=None
            Descriptive help text.
        """
        super(DataRepository, self).__init__(
            identifier=identifier,
            name=name,
            description=description
        )

    @abstractmethod
    def catalog(self) -> Iterable[DatasetHandle]:
        """Generator for a listing of all datasets that are available from the
        repository. Implementing classes may define additional, source specific
        query parameters.

        Returns
        -------
        iterable of openclean.data.source.DatasetHandle
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def dataset(self, identifier: str) -> DatasetHandle:
        """Get the handle for the dataset with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.

        Returns
        -------
        openclean.data.source.DatasetHandle
        """
        raise NotImplementedError()  # pragma: no cover
