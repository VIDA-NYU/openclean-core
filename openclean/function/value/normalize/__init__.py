# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Import normalization functions. Mainly for backward compatibility."""

from openclean.function.value.normalize.numeric import NumericNormalizer
from openclean.function.value.normalize.numeric import divide_by_total, max_abs_scale, min_max_scale
from openclean.function.value.normalize.numeric import DivideByTotal, MaxAbsScale, MinMaxScale
from openclean.function.value.normalize.text import TextNormalizer
