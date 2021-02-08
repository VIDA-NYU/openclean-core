# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Base classes for metadata stores and store factories."""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Optional


class MetadataStore(metaclass=ABCMeta):
    """Abstract class for metadata stores that maintain annotations for individual
    snapshots (datasets) in an archive.
    """
    def delete_annotation(
        self, key: str, column_id: Optional[int] = None,
        row_id: Optional[int] = None
    ):
        """Delete annotation with the given key for the object that is
        identified by the given combination of column and row identfier.

        Parameters
        ----------
        key: string
            Unique annotation key.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).
        """
        doc = self.read(column_id=column_id, row_id=row_id)
        if key in doc:
            del doc[key]
        self.write(doc=doc, column_id=column_id, row_id=row_id)

    def get_annotation(
        self, key: str, column_id: Optional[int] = None,
        row_id: Optional[int] = None, default_value: Optional[Any] = None
    ) -> Any:
        """Get annotation with the given key for the identified object. Returns
        the default vlue if no annotation with the given ey exists for the
        object.

        Parameters
        ----------
        key: string
            Unique annotation key.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).
        default_value: any, default=None
            Default value that is returned if no annotation with the given key
            exists for the identified object.

        Returns
        -------
        Any
        """
        doc = self.read(column_id=column_id, row_id=row_id)
        return doc.get(key, default_value)

    def has_annotation(
        self, key: str, column_id: Optional[int] = None,
        row_id: Optional[int] = None
    ) -> bool:
        """Test if an annotation with the given key exists for the identified
        object.

        Parameters
        ----------
        key: string
            Unique annotation key.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).

        Returns
        -------
        bool
        """
        return key in self.read(column_id=column_id, row_id=row_id)

    def list_annotations(
        self, column_id: Optional[int] = None, row_id: Optional[int] = None
    ) -> Dict:
        """Get all annotations for an identified object as a key,value-pair
        dictionary.

        Parameters
        ----------
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).
        """
        return self.read(column_id=column_id, row_id=row_id)

    @abstractmethod
    def read(
        self, column_id: Optional[int] = None, row_id: Optional[int] = None
    ) -> Dict:
        """Read the annotation dictionary for the specified object.

        Parameters
        ----------
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).

        Returns
        -------
        dict
        """
        raise NotImplementedError()  # pragma: no cover

    def set_annotation(
        self, key: str, value: Any, column_id: Optional[int] = None,
        row_id: Optional[int] = None
    ):
        """Set annotation value for an identified object.

        Parameters
        ----------
        key: string
            Unique annotation key.
        value: any
            Value that will be associated with the given key.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).
        """
        doc = self.read(column_id=column_id, row_id=row_id)
        doc[key] = value
        self.write(doc=doc, column_id=column_id, row_id=row_id)

    @abstractmethod
    def write(
        self, doc: Dict, column_id: Optional[int] = None,
        row_id: Optional[int] = None
    ):
        """Write the annotation dictionary for the specified object.

        Parameters
        ----------
        doc: dict
            Annotation dictionary that is being written to file.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).

        Returns
        -------
        dict
        """
        raise NotImplementedError()  # pragma: no cover


class MetadataStoreFactory(object):
    """Factory pattern for metadata stores. Metadata stores are created on a
    per-version basis. That is, each dataset snapshot has its own idependent
    metadata store.
    """
    @abstractmethod
    def get_store(self, version: int) -> MetadataStore:
        """Get the metadata store for the dataset snapshot with the given version
        identifier.

        Parameters
        ----------
        version: int
            Unique version identifier

        Returns
        -------
        openclean.data.metadata.base.MetadataStore
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def rollback(self, version: int):
        """Remove metadata for all dataset versions that are after the given
        rollback version.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.
        """
        raise NotImplementedError()  # pragma: no cover
