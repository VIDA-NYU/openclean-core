# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions for detecting and converting geo-spatial values from and to
Well-Known-Text representation representations.
"""

import logging
import re

from shapely.errors import WKTReadingError
from shapely.wkt import loads as wkt_parse

from openclean.function.value.base import PreparedFunction


"""Suppress extra information messages from shapely.
Based on: https://github.com/Toblerity/Shapely/issues/447
"""
logging.getLogger('shapely.geos').setLevel(logging.CRITICAL)


"""WKT geometries."""
WKT_GEOMETRIES = [
    'POINT',
    'LINESTRING',
    'POLYGON',
    'MULTIPOINT',
    'MULTILINESTRING',
    'MULTIPOLYGON',
    'GEOMETRYCOLLECTION',
]

_re_wkt_geometries = r'^({}) ?\(.+\)$'.format('|'.join(WKT_GEOMETRIES))


# -- Type parser --------------------------------------------------------------

def parse_wkt(value):
    """Parse a given value in Well-Known-Text representation. Attempts to parse
    the value using shapely's wkt methods. Returns the parsing result if
    successful or None if an error is raised.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being an integer.

    Returns
    -------
    shapely.geometry.base.BaseGeometry
    """
    # Use a regular expression first to exclude values that are not candidates.
    # The goal is to speed-up type detection by avoiding to run the expensive
    # parsing task on values that clearly are not WKT strings.
    try:
        if not re.match(_re_wkt_geometries, value):
            return None
    except TypeError:
        return None
    # Attempt convert the given value into a WKT object. Will raise a
    # WKTReadingError if the string is not a WKT string.
    try:
        geo_obj = wkt_parse(value)
        if geo_obj.is_valid:
            return geo_obj
    except (WKTReadingError, UnicodeDecodeError):
        return None


# -- Datatype detectors and classifiers ---------------------------------------

class WKTType(PreparedFunction):
    """Classifier for a geo-spatial values in Well-Known-Text format (WKT).
    Attempts to convert a given value into a WKT type. If successful, the
    classifier either returns a predefined label or a combination of the
    predefined label and the actual object type as suffix.
    """
    def __init__(
        self, label='wkt', include_type=True, delim=':', none_label=None
    ):
        """Initialize the label (prefix). The include type flag determines
        whether the returned labels will be a concatenation of the actual
        geo-spatial object type or only the label itself.

        Parameters
        ----------
        label: string
            Label (prefix) that is returned for values that can be parsed using
            the WKT parser.
        include_type: bool, default=True
            If True, the returned labels are concatenations of the given label
            prefix and the actual geo-spatial object type.
        delim: string, default=None
            Delimiter used to concat the given label and the geospatial type if
            the include type flag is True.
        none_label: scalar, default=None
            Default label that is returned for values that cannot be parsed
            using the WKT parser.
        """
        self.label = label
        self.include_type = include_type
        self.delim = delim
        self.none_label = none_label

    def eval(self, value):
        """Attempt to parse the given value using the WKT parser. If no error
        is raised a type label for the WKT type is returned.

        Parameters
        ----------
        value: scalar
            Scalar data value that is being classified.

        Returns
        -------
        string
        """
        geo_obj = parse_wkt(value)
        if geo_obj is not None:
            if self.include_type:
                return '{}{}{}'.format(self.label, self.delim, geo_obj.type)
            else:
                return self.label
        return self.none_label

    __call__ = eval


def is_wkt(value):
    """Test if a given value is in Well-Known-Text representation. Attempts to
    convert the value using shapely's wkt methods. Returns True if the value
    parses and false if parsing fails.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being an integer.

    Returns
    -------
    bool
    """
    return parse_wkt(value) is not None
