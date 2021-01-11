# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Repository for datasets that are extracted from web pages of the
Encyclopaedia Britannica.
"""

from html.parser import HTMLParser
from typing import IO, Iterable, List, Set, Tuple

import csv
import pandas as pd
import requests

from openclean.data.source.base import ColumnDescriptor, DatasetHandle, DataRepository
from openclean.data.types import Column


"""Identifier for the US cities dataset."""
US_CITIES = 'us_cities'

"""URL for the Encyclopaedia Britannica US Cities Web page."""
URL_US_CITIES = 'https://www.britannica.com/topic/list-of-cities-and-towns-in-the-United-States-2023068'


class CitiesDataset(DatasetHandle):
    """Handle for the US cities dataset which is the only dataset that is
    available from the Encyclopaedia Britannica web page.
    """
    def __init__(self):
        """Initialize the dataset descriptor."""
        super(CitiesDataset, self).__init__(
            identifier=US_CITIES,
            name='United States Cities',
            description='List of cities and towns in the United States',
            columns=[
                ColumnDescriptor(
                    identifier='city',
                    description='City name',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='state',
                    description='State name',
                    dtype='str'
                )
            ]
        )

    def load(self) -> pd.DataFrame:
        """Get data frame for the dataset with US city and state names.

        Returns
        -------
        pd.DataFrame
        """
        # Convert the collected results into a data frame with two columns.
        data = list()
        for state, cities in USCitiesHTMLParser().download_and_parse():
            for city in cities:
                data.append([city, state])
        return pd.DataFrame(
            data=data,
            columns=[
                Column(colid=0, name='city', colidx=0),
                Column(colid=1, name='state', colidx=1)
            ]
        )

    def write(self, file: IO):
        """Write the dataset to the given file. The output file format is a
        tab-delimited csv file with the column names as the first line.

        Parameters
        ----------
        file: file object
            File-like object that provides a write method.
        """
        # Write data to tab-delimited csv file.
        writer = csv.writer(file, delimiter='\t')
        columns = [c.identifier for c in self.columns]
        writer.writerow(columns)
        for state, cities in USCitiesHTMLParser().download_and_parse():
            for city in cities:
                writer.writerow([city, state])


class EncyclopaediaBritannica(DataRepository):
    """Repository handle for datasets that are extracted from web pages of the
    Encyclopaedia Britannica."""
    def __init__(self):
        """Initialize the data repository descriptor."""
        super(EncyclopaediaBritannica, self).__init__(
            identifier='britannica',
            name='Encyclopaedia Britannica',
            description='Datasets from Encyclopaedia Britannica Web Pages'
        )

    def catalog(self) -> Iterable[DatasetHandle]:
        """Generator for a listing of all datasets that are available from the
        repository.

        Returns
        -------
        iterable of openclean.data.source.DatasetHandle
        """
        return [CitiesDataset()]

    def dataset(self, identifier: str) -> DatasetHandle:
        """Get the handle for the dataset with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.

        Returns
        -------
        openclean.data.source.DatasetHandle
        """
        if identifier == US_CITIES:
            return CitiesDataset()
        raise ValueError("unknown identifier '{}'".format(identifier))


# -- Helper classes -----------------------------------------------------------

class USCitiesHTMLParser(HTMLParser):
    """Parser for the Encyclopaedia Britannica US Cities Web page. The parser
    extracts names of cities in the US from the web page together with the
    name of the state they appear in.
    """
    def __init__(self):
        """Initialize parser state that keeps track of the document tree and
        the collected results.
        """
        # Make sure to call the constructor of the super class.
        super(USCitiesHTMLParser, self).__init__()
        # States are identified by the following tad sequence:
        # <h2 class="h1"><a.
        self.states = None
        self._open_state = False
        # Lists of city names are identified by:
        # <ul class="topic-list">
        self._reading_cities = False

    def handle_endtag(self, tag: str):
        """Check for closing ul tags to stop accumulating city names for a
        state.
        """
        if tag == 'ul':
            self._reading_cities = False

    def handle_data(self, data):
        """Handle next data element."""
        if self._open_state == 1:
            self.states.append((data.strip(), set()))
            self._open_state = False
        elif self._reading_cities:
            self.states[-1][1].add(data.strip())

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str]]):
        """Handle next start tag in the HTML document."""
        # States are identified by <h2 class="h1"> followed by <a>
        if tag == 'h2':
            if ('class', 'h1') in attrs:
                self._open_state = True
        elif tag == 'a' and self._open_state:
            pass
        elif tag == 'ul' and ('class', 'topic-list') in attrs:
            self._reading_cities = True
        else:
            self._open_state = False

    def download_and_parse(self) -> List[Tuple[str, Set]]:
        """Download and parse of the web site. Returns a list of tuples with
        states and their city names.

        Returns
        -------
        list of tuple of string and set
        """
        # Parse the web page to get al list of US states and their cities.
        r = requests.get(URL_US_CITIES)
        r.raise_for_status()
        self.states = list()
        self.feed(r.text)
        return self.states
