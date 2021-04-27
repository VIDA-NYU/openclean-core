# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handles for operators that have been applied to a dataset. Handles are stored
in a log (provenance record) for the dataset. The operator handles contain all
the information that is necessary to reapply the opertor to a dataset version.
"""

from abc import abstractmethod
from typing import Dict, List, Optional, Union


from openclean.data.archive.base import ActionHandle
from openclean.data.schema import select_clause
from openclean.data.types import Columns, Scalar, DatasetSchema
from openclean.function.eval.base import Eval, EvalFunction, to_const_eval
from openclean.engine.object.function import FunctionHandle


"""Operator types."""

OP_COMMIT = 'commit'
OP_INSCOL = 'inscol'
OP_LOAD = 'load'
OP_SAMPLE = 'sample'
OP_UPDATE = 'update'


class OpHandle(ActionHandle):
    """The operator handle defines the interface for entries in the provenance
    log of a dataset. The defined methods are used to store the handle and to
    re-apply the operation using a evaluation function that is generated from
    the operator metadata.
    """
    def __init__(
        self, optype: str, schema: Optional[DatasetSchema] = None,
        columns: Optional[Columns] = None,
        func: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the operator metadata.

        Parameters
        ----------
        optype: string
            Unique operator type identifier.
        schema: list of string, default=None
            Column names in the dataset schema.
        columns: int, string, or list of int or string, default=None
            List of columns. This defines the columns that are affected (e.g.,
            updated) by the operation.
        func: scalar or openclean.engine.object.func.FunctionHandle
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
        self.schema = schema
        self.columns = columns
        self.func = func
        self.args = args
        self.sources = sources

    @property
    def is_insert(self) -> bool:
        """True if the operator type is 'inscol'.

        Returns
        -------
        bool
        """
        return self.optype == OP_INSCOL

    @property
    def is_update(self) -> bool:
        """True if the operator type is 'update'.

        Returns
        -------
        bool
        """
        return self.optype == OP_UPDATE

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        # Get list of column names from the schema. FOr insert command the list
        # of columns are actually strings already.
        colnames = self.columns
        if self.is_update:
            colnames, _ = select_clause(schema=self.schema, columns=colnames)
        doc = {'optype': self.optype, 'columns': colnames}
        if self.func is not None:
            if isinstance(self.func, FunctionHandle):
                f = self.func
                doc['name'] = f.label if f.label is not None else f.name
                if f.namespace is not None:
                    doc['namespace'] = f.namespace
            else:
                # Assume that func is a scalar value.
                doc['value'] = self.func
        if self.args is not None:
            doc['arguments'] = [{'name': k, 'value': v} for k, v in self.args.items()]
        if self.sources is not None:
            srcnames, _ = select_clause(schema=self.schema, columns=self.sources)
            doc['sources'] = srcnames
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


class CommitOp(OpHandle):
    """Handle for a user commit operation."""
    def __init__(self):
        """Initialize the action type in the super class."""
        super(CommitOp, self).__init__(optype=OP_COMMIT)

    def to_eval(self) -> EvalFunction:
        """The commit operator cannot be converted to an evaluation function.
        If an attempt is made to do so a runtime error is raised.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise RuntimeError('cannot re-apply commit')


class InsertOp(OpHandle):
    """Handle for an insert operation."""
    def __init__(
        self, schema: DatasetSchema, names: Union[str, List[str]], pos: Optional[int] = None,
        values: Optional[Union[Scalar, FunctionHandle]] = None,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an insert operator.

        Parameters
        ----------
        schema: list of string
            Column names in the dataset schema.
        names: string, or list(string)
            Names of the inserted columns.
        pos: int, default=None
            Insert position for the new columns. If None, the columns will be
            appended.
        values: scalar or openclean.engine.object.func.FunctionHandle, default=None
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
            optype=OP_INSCOL,
            schema=schema,
            columns=names,
            func=values,
            args=args,
            sources=sources
        )
        self.pos = pos

    @property
    def names(self) -> Union[str, List[str]]:
        """Synonym for accessing the columns (which are the names of the inserted
        columns for an inscol operator).

        Returns
        -------
        string or list of string
        """
        return self.columns

    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the handle.

        Returns
        -------
        dict
        """
        doc = super(InsertOp, self).to_dict()
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
        if not isinstance(self.func, FunctionHandle):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return to_const_eval(self.func)
        # Return an evaluation function that wraps the callable. The input for
        # that function comes from the list of sources.
        return Eval(columns=self.sources, func=self.func, args=self.args)


class LoadOp(OpHandle):
    """Handle for a load operation."""
    def __init__(self):
        """Initialize the action type in the super class."""
        super(LoadOp, self).__init__(optype=OP_LOAD)

    def to_eval(self) -> EvalFunction:
        """The load operator cannot be converted to an evaluation function. If
        an attempt is made to do so a runtime error is raised.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise RuntimeError('cannot re-apply load')


class SampleOp(OpHandle):
    """Handle for a dataset sample operation."""
    def __init__(self, args: Optional[Dict] = None):
        """Initialize the action type in the super class.

        Parameters
        ----------
        args: dict, default=None
            Arguments for the sample operation.
        """
        super(SampleOp, self).__init__(optype=OP_SAMPLE, args=args)

    def to_eval(self) -> EvalFunction:
        """The sample operator cannot be converted to an evaluation function.
        If an attempt is made to do so a runtime error is raised.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        raise RuntimeError('cannot re-apply sample')


class UpdateOp(OpHandle):
    """Handle for an update operation."""
    def __init__(
        self, schema: DatasetSchema, columns: Columns, func: FunctionHandle,
        args: Optional[Dict] = None, sources: Optional[Columns] = None
    ):
        """Initialize the metadata for an update operator.

        Parameters
        ----------
        schema: list of string
            Column names in the dataset schema.
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        func: openclean.engine.object.func.FunctionHandle
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
            optype=OP_UPDATE,
            schema=schema,
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
        if not isinstance(self.func, FunctionHandle):
            # If the function is not a function handle it is assumed to be
            # a scalar value. Wrap that value in a Constant evaluation function.
            return to_const_eval(self.func)
        # Create an evaluation function that wraps the callable. The input
        # columns depend on whether sources are given or not.
        columns = self.sources if self.sources is not None else self.columns
        return Eval(columns=columns, func=self.func, args=self.args)
