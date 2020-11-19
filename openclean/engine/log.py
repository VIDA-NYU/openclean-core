# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handles for operators that have been applied to a dataset. Handles are stored
in a log (provenance record) for the dataset. The operator handles contain all
the information that is necessary to reapply the opertor to a dataset version.
"""

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from openclean.data.types import Columns, Scalar
from openclean.function.eval.base import Const, Eval, EvalFunction
from openclean.engine.library.func import FunctionHandle


# -- Opration handles ---------------------------------------------------------

class OpHandle(metaclass=ABCMeta):
    """The operator handle defines the interface for entries in the provenance
    log of a dataset. The defined methods are used to store the handle and to
    re-apply the operation using a evaluation function that is generated from
    the operator metadata.
    """
    def __init__(
        self, optype: str, columns: Columns,
        func: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the operator metadata.

        Parameters
        ----------
        optype: string
            Unique operator type identifier.
        columns: int, string, or list of int or string
            List of columns. This defines the columns that are affected (e.g.,
            updated) by the operation.
        func: scalar or openclean.engine.library.func.FunctionHandle
            Scalar value or library function that is used to generate values for
            the modified column(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.
        """
        self.optype = optype
        self.columns = columns
        self.func = func
        self.args = args
        self.sources = sources

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        doc = {'type': self.optype, 'columns': self.columns}
        if self.func is not None:
            if isinstance(self.func, FunctionHandle):
                doc['func'] = self.func.to_descriptor()
            else:
                doc['func'] = self.func
        if self.args is not None:
            doc['arguments'] = self.args
        if self.sources is not None:
            doc['sources'] = self.sources
        return doc

    @abstractmethod
    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise NotImplementedError()


class InsertOp(OpHandle):
    """Handle for an insert operation."""
    def __init__(
        self, names: Union[str, List[str]], pos: Optional[int] = None,
        values: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an insert operator.

        Parameters
        ----------
        names: string, or list(string)
            Names of the inserted columns.
        pos: int, default=None
            Insert position for the new columns. If None, the columns will be
            appended.
        values: scalar or openclean.engine.library.func.FunctionHandle, default=None
            Single value, tuple of values, or library function that is used to
            generate the values for the inserted column(s). If no default is
            specified all columns will contain None.
        func: callable or openclean.function.eval.base.EvalFunction
            Callable that accepts a data frame row as the only argument and
            outputs a (modified) list of value(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.
        """
        super(InsertOp, self).__init__(
            optype='update',
            columns=names,
            func=values,
            args=args,
            sources=sources
        )
        self.pos = pos

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        doc = super(self).to_dict()
        doc['pos'] = self.pos
        return doc

    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        # Return nothing if the associated function is None.
        if self.func is None:
            # Return nothing if the associated function is None.
            return None
            # Use the update columns as input columns if no source columns are
            # specified explicitly.
        if not callable(self.func):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return Const(self.func)
        # Return an evaluation function that wraps the callable. The input for
        # that function comes from the list of sources.
        return Eval(columns=self.sources, func=self.func, args=self.args)


class UpdateOp(OpHandle):
    """Handle for an update operation."""
    def __init__(
        self, columns: Columns, func: FunctionHandle, args: Optional[Dict] = None,
        sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an update operator.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        func: openclean.engine.library.func.FunctionHandle
            Library function that is used to generate modified values for the
            updated column(s).
        args: dict, default=None
            Additional keyword arguments that are passed to the callable together
            with the column values that are extracted from each row.
        sources: int, string, or list(int or string), default=None
            List of source columns from which the input values for the
            callable are extracted.
        """
        super(UpdateOp, self).__init__(
            optype='update',
            columns=columns,
            func=func,
            args=args,
            sources=sources
        )

    def to_eval(self) -> EvalFunction:
        """Get an evaluation function instance that can be used to re-apply the
        represented operation on a dataset version.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        if self.func is None:
            # Return nothing if the associated function is None.
            return None
            # Use the update columns as input columns if no source columns are
            # specified explicitly.
        if not callable(self.func):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return Const(self.func)
        # Create an evaluation function that wraps the callable. The input
        # columns depend on whether sources are given or not.
        columns = self.sources if self.sources is not None else self.columns
        return Eval(columns=columns, func=self.func, args=self.args)


# -- Operation log ------------------------------------------------------------

@dataclass
class LogEntry:
    """Entry in an operation log. Maintains the datsset version identifier and
    the handle for the action that created the dataset version.
    """
    action: OpHandle
    version: int


class OperationLog(object):
    """The operation log maintains a list of entries containing provenance
    information for each verion of a dataset.
    """
    def __init__(self):
        """Initialize the internal entry list."""
        self.entries = list()

    def add(self, version: int, action: OpHandle):
        """Append a record to the log.

        Parameters
        ----------
        version: int
            Dataset snapshot version identifier.
        action: openclean.engine.log.OpHandle
            Handle for the operation that created the dataset snapshot.
        """
        self.entries.append(LogEntry(version=version, action=action))
