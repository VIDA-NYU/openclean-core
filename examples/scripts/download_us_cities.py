# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from openclean.data.downloader.britannica.cities import USCitiesHTMLParser


cities = USCitiesHTMLParser().download()

print(cities)
