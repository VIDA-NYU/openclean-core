# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the abstract unique column combination finder  base class."""

from typing import List

import pandas as pd

from openclean.data.types import Columns
from openclean.profiling.constraints.ucc import UniqueColumnCombinationFinder


class UCCDummy(UniqueColumnCombinationFinder):
    """Dummy implementation of the unique column combination finder."""
    def run(self, df: pd.DataFrame) -> List[Columns]:
        return df.columns


def test_dummy_ucc_finder(employees):
    """Test unique column combination finder."""
    assert UCCDummy().run(employees) is not None
