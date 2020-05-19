# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Downloader for information about countries in the world provided by the
restcountries project.
"""

import pandas as pd
import requests

from openclean.data.downloader.base import RepositoryDownloader
from openclean.data.downloader.dataset import DatasetDescriptor


"""Identifier for the countries dataset."""
COUNTRIES = 'countries'


class RestcountriesDownloader(RepositoryDownloader):
    """Downloader for the restcountries project."""
    def datasets(self):
        """Get a description of 'countries' datasets that is available for
        download from this repository.

        Returns
        -------
        list(openclean.data.downloader.dataset.DatasetDescriptor)
        """
        return [DatasetDescriptor({
            'id': COUNTRIES,
            'name': 'Countries of the World',
            'description': (
                'Information about all countries in the world provided by the '
                'restcountries project'
            ),
            'columns': [
                {
                    'name': 'name',
                    'description': 'Country name'
                },
                {
                    'name': 'alpha2Code',
                    'description': 'ISO 3166-1 2-letter country code'
                },
                {
                    'name': 'alpha3Code',
                    'description': 'ISO 3166-1 3-letter country code'
                },
                {
                    'name': 'capital',
                    'description': 'Capital city'
                },
                {
                    'name': 'region',
                    'description': 'Africa, Americas, Asia, Europe, or Oceania'
                },
                {
                    'name': 'subregion',
                    'description': 'Sub-region with the country region'
                }
            ]
        })]

    def download(self, datasets=None, properties=None):
        """Download the complete country listing provided by the restcountries
        project. Rauses a ValueError if the dataset name is not None or equal
        to 'countries'.

        Returns a dictionary with a single entry 'countries'.

        Parameters
        ----------
        datasets: string or list of string, default=None
            List of datasets that are downladed. This downloader provodes only
            a single dataset 'countries'. This dataset will be downloaded by
            default.
        properties: dict, default=None
            Additional properties for the download. The downloaded currently
            does not support additional properties. Included for API
            completeness.

        Returns
        -------
        dict

        Raises
        ------
        ValueError
        """
        # Ensure that the value for the datasets parameter is valid.
        if datasets is not None:
            if isinstance(datasets, list):
                for ds in datasets:
                    if ds != COUNTRIES:
                        raise ValueError('unknown dataset {}'.format(ds))
            elif datasets != COUNTRIES:
                raise ValueError('unknown dataset {}'.format(datasets))
        # Download the full dataset from API endpoint.
        r = requests.get('https://restcountries.eu/rest/v2/all')
        r.raise_for_status()
        doc = r.json()
        # Convert Json object to data frame.
        data = list()
        columns = [
            'name',
            'alpha2Code',
            'alpha3Code',
            'capital',
            'region',
            'subregion'
        ]
        for obj in doc:
            data.append([obj[key] for key in columns])
        return {COUNTRIES: pd.DataFrame(data=data, columns=columns)}
