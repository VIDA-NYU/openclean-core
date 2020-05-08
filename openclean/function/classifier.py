# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes to classify for scalar values and to compute summaries over
data frames that have class labels assigned to their data rows.
"""


class ValueClassifier(object):
    """The value classifier evaluates a list of predicates or conditions on a
    given value (scalar or tuple). Each predicate is associated with a class
    label. The corresponding class label for the first predicate that is
    satisfied by the value is returned as the classification result. If no
    predicate is satisfied by a given value the result is either a default
    label or a ValueError is raised if the raise error flag is set to True.
    """
    def __init__(
        self, classifier, labels, truth_values=None, default_label=None,
        raise_error=False
    ):
        """Initialize the object properties. Raises an error if the number of
        elements in the predicates list and labels list (and the truth values
        list of given) to not match.

        Parameters
        ----------
        classifier: callable or list(callable)
            List of callables that accept a scalar value as input and that
            return a scalar value that is of same type as the corresponding
            value in the list of truth values.
        labels: scalar or list(scalar)
            Class labels that are associated with the predicates.
        truth_values: list(scalar), optional
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
        # Convert type checkers and labels to lists
        if not isinstance(classifier, list):
            classifier = list([classifier])
        if not isinstance(labels, list):
            labels = list([labels])
        if len(classifier) != len(labels):
            raise ValueError('incompatible lists for predicates and labels')
        if truth_values is not None:
            if len(truth_values) != len(classifier):
                msg = 'incompatible lists for predicates and truth values'
                raise ValueError(msg)
        else:
            truth_values = [True] * len(classifier)
        # Merge the three lists into one list where the elements are triples of
        # type checker, label, truth value
        self.classifiers = list()
        for i in range(len(classifier)):
            triple = (classifier[i], labels[i], truth_values[i])
            self.classifiers.append(triple)
        self.default_label = default_label
        self.raise_error = raise_error

    def __call__(self, value):
        """Make the classifier callable for use as value update function.

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
        return self.eval(value)

    def eval(self, value):
        """Evaluate the predicates on the given value. Predicates are evaluated
        in the order of their appearance in the list. Returns the associated
        label for the first predicate that is satisfied. If no predicate is
        satisfied the result is the default label. If the raise error flag is
        True an error is raised instead.

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
        for pred, label, truth_value in self.classifiers:
            if pred(value) == truth_value:
                return label
        # No predicate was satisfied. Raise an error if the raise error flag is
        # True. Otherwise return the default label.
        if self.raise_error:
            raise ValueError('no match for {}'.format(value))
        else:
            return self.default_label
