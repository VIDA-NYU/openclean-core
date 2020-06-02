# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes to classify for scalar values and to compute summaries over
data frames that have class labels assigned to their data rows.
"""

from openclean.function.value.base import CallableWrapper, ValueFunction


class ClassLabel(ValueFunction):
    """Classifier for a single type. Assigns a pre-defined class label to
    values that belong to the type that is represented by the classifier.
    Type membership is represented by a given predicate. All values that do not
    satisfy the predicate are assigned a pre-defined non-label.
    """
    def __init__(self, predicate, label, truth_value=True, none_label=None):
        """Initialize the type predicate and label. The truth value defines the
        return value of the predicate that is considered as satisfiyng the
        predicate.

        Parameters
        ----------
        predicate: callable or openclean.function.values.base.ValueFunction
            Predicate that is evaluated on values to determine type membership.
        label: string
            Label that is returned for values that satisfy the predicate.
        truth_value: scalar, default=True
            Return value of the predicate that is considered to signal that a
            value satisfies the predicate.
        none_label: scalar, default=None
            Default label that is returned for values that do not satisfy the
            predicate.

        Raises
        ------
        ValueError
        """
        # Ensure that the predicate is a ValueFunction
        if not isinstance(predicate, ValueFunction):
            # Wrap the predicate inside a ValueFunction. This will raise a
            # ValueError if the predicate is not callable.
            predicate = CallableWrapper(predicate)
        self.predicate = predicate
        self.label = label
        self.truth_value = truth_value
        self.none_label = none_label

    def eval(self, value):
        """Evaluate the function on a given value. The value may either be a
        scalar or a tuple. The value will be from the list of values that was
        passed to the object in the prepare call.

        The return value of the function is implementation dependent.

        Parameters
        ----------
        value: scalar or tuple
            Scalar data value that is being classified.

        Returns
        -------
        scalar or tuple
        """
        if self.predicate.eval(value) == self.truth_value:
            return self.label
        else:
            return self.none_label

    __call__ = eval

    def prepare(self, values):
        """Call the prepare method of the associated predicate.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        self.predicate.prepare(values)
        return self


class ValueClassifier(ValueFunction):
    """The value classifier evaluates a list of predicates or conditions on a
    given value (scalar or tuple). Each predicate is associated with a class
    label. The corresponding class label for the first predicate that is
    satisfied by the value is returned as the classification result. If no
    predicate is satisfied by a given value the result is either a default
    label or a ValueError is raised if the raise error flag is set to True.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the object properties. Raises an error if the number of
        elements in the predicates list and labels list (and the truth values
        list of given) to not match.

        Parameters
        ----------
        args: list of callable or openclean.function.value.base.ValueFunction
            List of functions that accept a scalar value as input and that
            return a class label as output.
        labels: scalar or list(scalar)
            Class labels that are associated with the predicates.
        truth_value: list(scalar), optional
            Return value of the predicate that is considered as True, i.e., the
            predicate is satisfied.
        default_label: scalar, optional
            Default class label that is returned if no predicate is satisfied.
        raise_error: bool, optional
            Raise an error if no predicate is satisfied instead of returning
            the default label.

        Raises
        ------
        ValueError
        """
        self.classifiers = args
        self.none_label = kwargs.get('none_label')
        self.default_label = kwargs.get('default_label')
        self.raise_error = kwargs.get('raise_error', False)

    def eval(self, value):
        """Evaluate the classifiers on the given value. The classifier are
        evaluated in the order of their appearance in the list. Returns the
        associated label for the first predicate that is satisfied (i.e., the
        classifier returns a label that is not the none label). If none of the
        classifier predicates is satisfied the result is the default label. If
        the raise error flag is True an error is raised instead.

        Parameters
        ----------
        value: scalar
            Scalar data value that is being classified.

        Returns
        -------
        scalar

        Raises
        ------
        ValueError
        """
        for classifier in self.classifiers:
            label = classifier.eval(value)
            if label != self.none_label:
                return label
        # No predicate was satisfied. Raise an error if the raise error flag is
        # True. Otherwise return the default label.
        if self.raise_error:
            raise ValueError('no match for {}'.format(value))
        else:
            return self.default_label

    __call__ = eval

    def prepare(self, values):
        """Call the prepare method of the associated classifiers.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        for classifier in self.classifiers:
            classifier.prepare(values)
        return self
