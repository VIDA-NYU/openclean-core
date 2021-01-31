# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from openclean.data.load import dataset
from openclean.operator.transform import ignore, select
from openclean.function.eval.predicate.row import IsEmpty


df = dataset('../data/311.tsv')
df = ignore(df, IsEmpty('intersection_street_2'))
df = select(df, ['descriptor', 'borough', 'city', 'intersection_street_2'])

df.to_csv(
    '../data/311-descriptor.csv',
    index=False,
    header=['descriptor', 'borough', 'city', 'street']
)
