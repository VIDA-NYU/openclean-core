# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Default feature embedding for strings."""

from openclean.embedding.feature.base import FeatureEmbedding
from openclean.embedding.feature.character import (
    digits_fraction, letters_fraction, spec_char_fraction, unique_fraction,
    whitespace_fraction
)
from openclean.embedding.feature.frequency import NormalizedFrequency
from openclean.embedding.feature.length import NormalizedLength


class StandardEmbedding(FeatureEmbedding):
    """Instance of the feature embedding function that uses a default set of
    seven value features to compute feature vectors. The computed features are:
    - normalized value length
    - normalized value frequency
    - uniqueness of characters in the value string
    - fraction of letter characters in the value string
    - fraction of digits in the value string
    - fraction of speical characters in the value string (not digit, letter, or
      whitespace)
    - fraction of whitespace characters in the value string
    """
    def __init__(self):
        """Initialize the list of default value feature functions."""
        super(StandardEmbedding, self).__init__(
            features=[
                NormalizedLength(),
                NormalizedFrequency(),
                unique_fraction,
                letters_fraction,
                digits_fraction,
                spec_char_fraction,
                whitespace_fraction
            ]
        )


class UniqueSetEmbedding(FeatureEmbedding):
    """Instance of the feature embedding function for nique value stes. This
    embedding ignores value frequencies. It uses a set of six value features to
    compute feature vectors. The computed features are:
    - normalized value length
    - uniqueness of characters in the value string
    - fraction of letter characters in the value string
    - fraction of digits in the value string
    - fraction of speical characters in the value string (not digit, letter, or
      whitespace)
    - fraction of whitespace characters in the value string
    """
    def __init__(self):
        """Initialize the list of default value feature functions."""
        super(UniqueSetEmbedding, self).__init__(
            features=[
                NormalizedLength(),
                unique_fraction,
                letters_fraction,
                digits_fraction,
                spec_char_fraction,
                whitespace_fraction
            ]
        )
