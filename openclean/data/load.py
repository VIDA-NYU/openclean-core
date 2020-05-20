# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import csv
import gzip
import pandas as pd

from openclean.data.column import Column


def dataset(filename):
    if filename.endswith('.tsv') or filename.endswith('.tsv.gz'):
        delimiter = '\t'
    else:
        delimiter = ','
    if filename.endswith('.gz'):
        csvfile = gzip.open(filename, 'rt')
    else:
        csvfile = open(filename, 'r')
    reader = csv.reader(csvfile, delimiter=delimiter)
    columns = list()
    for col_name in next(reader):
        columns.append(Column(colid=len(columns), name=col_name.strip()))
    data = list()
    while True:
        try:
            data.append(next(reader))
        except StopIteration:
            break
    csvfile.close()
    return pd.DataFrame(
        data=data,
        columns=columns,
        index=range(len(data))
    )
