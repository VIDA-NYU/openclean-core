# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pandas as pd

df = pd.read_csv('../data/2010-2011_Class_Size_-_School-level_detail.csv')

df = df.filter(
    items=[
        'BOROUGH',
        'SCHOOL CODE',
        'GRADE ',
        'AVERAGE CLASS SIZE',
        'SIZE OF SMALLEST CLASS',
        'SIZE OF LARGEST CLASS'
    ])

df = df.sample(n=100)

df.to_csv(
    '../data/school_level_detail.csv',
    index=False,
    header=[
        'borough',
        'school_code',
        'grade',
        'avg_class_size',
        'min_class_size',
        'max_class_size'
    ]
)
