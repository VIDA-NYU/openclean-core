# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of openclean operators that represent (or are similar to) VizUAL
operators.
"""

from openclean.operator.transform.delete import delete_columns, delete_rows  # noqa: F401, E501
from openclean.operator.transform.insert import inscol as insert_column  # noqa: F401, E501
from openclean.operator.transform.insert import insrow as insert_row  # noqa: F401, E501
from openclean.operator.transform.move import move_columns, move_rows  # noqa: F401, E501
from openclean.operator.transform.rename import rename_columns  # noqa: F401
from openclean.operator.transform.select import filter_columns  # noqa: F401
from openclean.operator.transform.sort import sort_dataset  # noqa: F401
from openclean.operator.transform.update import update_cell  # noqa: F401
