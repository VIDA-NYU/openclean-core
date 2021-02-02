# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for generating token signatures."""

import pandas as pd
import pytest

from openclean.operator.map.groupby import groupby
from openclean.profiling.pattern.token_signature import token_signature


DATA = pd.DataFrame(
    data=[['street', 'str'], ['street', 'st'], ['avenue', 'ave'], ['avenue', 'av']],
    columns=['long_name', 'short_name']
)


@pytest.mark.parametrize(
    'result,include_key',
    [
        ([{'str', 'st'}, {'ave', 'av'}], False),
        ([{'street', 'str', 'st'}, {'avenue', 'ave', 'av'}], True)
    ]
)
def test_create_signature_from_grouping(include_key, result):
    """Test creating a token signature from a data frame grouping."""
    signature = token_signature(
        grouping=groupby(df=DATA, columns='long_name'),
        columns=['short_name'],
        include_key=include_key
    )
    assert len(signature) == len(result)
    for entry in result:
        assert entry in signature
