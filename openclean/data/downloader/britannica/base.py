# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Downloader for datasets that are extracted from web pages of the
Encyclopaedia Britannica.
"""

from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from openclean.data.downloader.base import RepositoryDownloader
from openclean.data.downloader.base import DatasetDescriptor
from openclean.data.downloader.britannica.cities import USCitiesHTMLParser


"""Identifier for the US cities dataset."""
US_CITIES = 'us_cities'
# List of valid dataset names.
DATASETS = {US_CITIES}


class EncyclopaediaBritannicaDownloader(RepositoryDownloader):
    """Downloader for datasets that are extracted from web pages of the
    Encyclopaedia Britannica.
    """
    def datasets(self) -> List[DatasetDescriptor]:
        """Get a description of the datasets that are available for download
        from the Encyclopaedia Britannica web pages.

        Returns
        -------
        list(openclean.data.downloader.base.DatasetDescriptor)
        """
        return [
            DatasetDescriptor({
                'id': US_CITIES,
                'name': 'United States Cities',
                'description': (
                    'List of cities and towns in the United States'
                ),
                'columns': [
                    {
                        'name': 'city',
                        'description': 'City name'
                    },
                    {
                        'name': 'state',
                        'description': 'State name'
                    }
                ],
                'primaryKey': ['city', 'state']
            })
        ]

    def download(
        self, datasets: Optional[Union[str, List[str]]] = None,
        properties: Optional[Dict] = None
    ) -> List[Tuple[DatasetDescriptor, pd.DataFrame]]:
        """Download dataset from the Encyclopaedia Britannica web pages. Raises
        a ValueError if the dataset name is unknown.

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
        list(
            tuple(
                openclean.data.downloader.base.DatasetDescriptor,
                pandas.DataFrame
            )
        )

        Raises
        ------
        ValueError
        """
        # Ensure that the values for the datasets parameter are valid.
        if datasets is None:
            datasets = DATASETS
        else:
            if not isinstance(datasets, list):
                datasets = [datasets]
            for ds in datasets:
                if ds not in DATASETS:
                    raise ValueError('unknown dataset {}'.format(ds))
        # Download the individual datasets.
        downloads = list()
        for ds in datasets:
            if ds == US_CITIES:
                downloads.append((
                    self.datasets()[0],
                    USCitiesHTMLParser().download()
                ))
        return downloads
