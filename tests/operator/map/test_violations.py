# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the violations operators"""

from openclean.operator.collector.count import distinct
from openclean.operator.map.violations import fd_violations, key_violations


def test_fdviolation_operator(agencies):
    """ Test fd violation works correctly"""
    lhs = ['borough', 'state']
    rhs = 'agency'
    fd = fd_violations(df=agencies, lhs=lhs, rhs=rhs)
    assert fd.get(('BK', 'NY')).shape[0] == 6
    assert fd.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') not in fd.keys()


def test_fdviolation2_operator(agencies):
    """ Test fd violation works correctly with inverted columns"""
    rhs = ['borough', 'state']
    lhs = 'agency'
    fd = fd_violations(df=agencies, lhs=lhs, rhs=rhs)
    assert len(fd) == 3
    assert fd.get(('NYPD')).shape[0] == 2
    assert fd.get(('FDNY')).shape[0] == 3
    assert 'DSNY' not in fd.keys()


def test_fdviolations_parking(parking):
    """Ensure that all returned fd violations for the parking tickets FD
    'Meter Number' -> 'Street' have at least two distinct values in the
    'Street' attribute.
    """
    # Get violations for the FD 'Meter Number' -> 'Street'
    groups = fd_violations(parking, lhs='Meter Number', rhs='Street')
    assert len(groups) > 0
    # For each group count the number of distinct values in the 'Street'
    # attribute.
    for key in groups:
        values = distinct(groups.get(key), 'Street')
        assert len(values) > 1


def test_keyviolations_parking(parking):
    """Ensure that there exist 3 duplicates in the 'Plate ID' column
    and 2 of them are at the same meter
    """
    groups = key_violations(parking, columns='Plate ID')
    assert len(groups) == 3

    # also ensures nan duplicates don't break the violation operations
    groups = key_violations(parking, columns=['Street'])
    assert len(groups) == 21

    groups = key_violations(parking, columns=['Plate ID', 'Meter Number'])
    assert len(groups) == 2


def test_nviolations_parking(parking):
    """Ensure we can select groups with exactly n=2 violations"""
    groups = key_violations(df=parking, columns='Plate ID', n=2)
    assert len(groups) == 3

    groups = key_violations(df=parking, columns=['Plate ID', 'Meter Number'], n=2)
    assert len(groups) == 2

'''
Performance tests, need only be run with additional function profiling stats being recorded
'''
# def test_old_fdviolations_performance():
#     # profile test
#     import os
#     from openclean.data.load import stream
#     df = stream(os.path.join('../../../examples/notebooks/NYCRestaurantInspections/data/', '43nn-pn8j.tsv.gz')).to_df()
#
#     from openclean.operator.map.violations import Violations
#     from collections import Counter
#     import pandas as pd
#
#     def duplicates(meta):
#         return len(meta) > 1
#
#     class old_fd_violations(Violations):
#         def __init__(self, lhs, rhs, func=None, having=None):
#             super(old_fd_violations, self).__init__(lhs, rhs, func, having)
#
#         def _transform(self, df):
#             groups = dict()
#             meta = dict()
#             for index, row in df.iterrows():
#                 value = tuple(row[self.lhs].values) if isinstance(row[self.lhs], pd.Series) else row[self.lhs]
#                 if isinstance(value, list):
#                     value = tuple(value)
#                 if value not in groups:
#                     groups[value] = list()
#                     meta[value] = Counter()
#                 groups[value].append(index)
#
#                 meta_value = tuple(row[self.rhs].values)
#                 meta[value] += Counter([tuple(meta_value.tolist())]) if isinstance(meta_value, pd.Series) else Counter(
#                     [meta_value])
#
#             return groups, meta
#
#     old_fd_violations('CAMIS', ['DBA', 'BORO'], None, duplicates).map(df)
#
# def test_new_fdviolations_performance():
#     # profile this test too
#     from openclean.operator.map.violations import fd_violations
#     import os
#     from openclean.pipeline import stream
#     df = stream(os.path.join('../../../examples/notebooks/NYCRestaurantInspections/data/', '43nn-pn8j.tsv.gz')).to_df()
#
#     fd_violations(df, 'CAMIS', ['DBA', 'BORO'])
