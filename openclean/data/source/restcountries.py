# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data repository for information about countries in the world provided by the
restcountries project.
"""

from typing import Dict, IO, Iterable

import csv
import pandas as pd
import requests

from openclean.data.source.base import ColumnDescriptor, DatasetHandle, DataRepository
from openclean.data.types import Column


"""Identifier for the countries dataset."""
COUNTRIES = 'countries'


class CountriesDataset(DatasetHandle):
    """Handle for the countries of the word dataset which is the only dataset
    that is available from the restcountries project.
    """
    def __init__(self):
        """Initialize the dataset descriptor."""
        super(CountriesDataset, self).__init__(
            identifier=COUNTRIES,
            name='Countries of the World',
            description=(
                'Information about all countries in the world provided by '
                'the restcountries project'
            ),
            columns=[
                ColumnDescriptor(
                    identifier='name',
                    description='Country name',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='alpha2Code',
                    description='ISO 3166-1 2-letter country code',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='alpha3Code',
                    description='ISO 3166-1 3-letter country code',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='capital',
                    description='Capital city',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='region',
                    description='World region',
                    dtype='str'
                ),
                ColumnDescriptor(
                    identifier='subregion',
                    description='Sub-region with the country region',
                    dtype='str'
                )
            ]
        )

    def load(self) -> pd.DataFrame:
        """Download the complete country listing provided by the restcountries
        project. Returns a data frame for the dataset.

        Returns
        -------
        pd.DataFrame
        """
        # Download the full dataset from API endpoint.
        doc = download_restcountries()
        # Convert Json object to data frame.
        columns = list()
        for col in self.columns:
            cid = len(columns)
            columns.append(Column(colid=cid, name=col.identifier, colidx=cid))
        data = list()
        for obj in doc:
            data.append([obj[c] for c in columns])
        return pd.DataFrame(data=data, columns=columns)

    def write(self, file: IO):
        """Write the dataset to the given file. The output file format is a
        tab-delimited csv file with the column names as the first line.

        Parameters
        ----------
        file: file object
            File-like object that provides a write method.
        """
        # Download the full dataset from API endpoint.
        doc = download_restcountries()
        # Write data to tab-delimited csv file.
        writer = csv.writer(file, delimiter='\t')
        columns = [c.identifier for c in self.columns]
        writer.writerow(columns)
        for obj in doc:
            writer.writerow([obj[c] for c in columns])


class RestcountriesRepository(DataRepository):
    """Repository handle for the restcountries project."""
    def __init__(self):
        """Initialize the data repository descriptor."""
        super(RestcountriesRepository, self).__init__(
            identifier='restcountries',
            name='restcountries.eu',
            description='Information about countries in the world'
        )

    def catalog(self) -> Iterable[DatasetHandle]:
        """Generator for a listing of all datasets that are available from the
        repository.

        Returns
        -------
        iterable of openclean.data.source.DatasetHandle
        """
        return [CountriesDataset()]

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
        if identifier == COUNTRIES:
            return CountriesDataset()
        raise ValueError("unknown identifier '{}'".format(identifier))


# -- Helper functions ---------------------------------------------------------

def download_restcountries() -> Dict:
    """Download the data from the restcountries project as a single Json
    document. Raises an error if the download is not successful.
    """
    # Download the full dataset from API endpoint.
    r = requests.get('https://restcountries.eu/rest/v2/all')
    # Raise an error if the download was not successful.
    r.raise_for_status()
    # Return the Json body from the response.
    return r.json()
