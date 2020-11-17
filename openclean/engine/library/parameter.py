# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Import for parameter declarations from flowserv. The constructor for each
parameter takes at least the following arguments:

    name: string
        Unique parameter identifier
    index: int, default=0
        Index position of the parameter (for display purposes).
    label: string, default=None
        Human-readable parameter name.
    help: string, default=None
        Descriptive text for the parameter.
    default: any, default=None
        Optional default value.
    required: bool, default=False
        Is required flag.
"""
from flowserv.model.parameter.base import Parameter  # noqa: F401
from flowserv.model.parameter.boolean import Bool  # noqa: F401
from flowserv.model.parameter.enum import Select, Option  # noqa: F401
from flowserv.model.parameter.files import File  # noqa: F401
from flowserv.model.parameter.list import Array  # noqa: F401
from flowserv.model.parameter.numeric import Boundary, Int, Float  # noqa: F401
from flowserv.model.parameter.record import Record  # noqa: F401
from flowserv.model.parameter.string import String  # noqa: F401
