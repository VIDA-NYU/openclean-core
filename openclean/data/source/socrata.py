# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data repository for accessing datasets via the Socrata Open Data API."""

from io import BytesIO
from refdata.base import Descriptor, DatasetDescriptor
from typing import Dict, IO, Iterable, List, Optional, Tuple

import pandas as pd
import requests


"""Base Urls for the different Socrata Data Discovery API catalogs."""
API_URLS = [
    'http://api.us.socrata.com/api/catalog/v1',
    'http://api.eu.socrata.com/api/catalog/v1'
]

LIMIT = 10000


class SODADataset(DatasetDescriptor):
    """Handle for a SODA dataset."""
    def __init__(self, doc: Dict, app_token: Optional[str] = None):
        """Initialize the dataset descriptor from the metadata dictionary that
        is available via the Socarata Data Discovery API.

        Parameters
        ----------
        doc: dict
            Metadata dictionary for the dataset.
        app_token: string, default=None
            Optional application token to be included in the header for all API
            requests.
        """
        resource = doc.get('resource')
        columns = zip(
            resource.get('columns_field_name'),
            resource.get('columns_name'),
            resource.get('columns_description'),
            resource.get('columns_datatype')
        )
        super(SODADataset, self).__init__({
            'id': doc.get('resource').get('id'),
            'name': doc.get('resource').get('name'),
            'description': doc.get('resource').get('description'),
            'schema': [{
                'id': id,
                'name': name,
                'description': desc,
                'dtype': dtype
            } for id, name, desc, dtype in columns]
        })
        # Maintain the Socrata domain for the dataset.
        self.domain = doc.get('metadata').get('domain')
        # Create url to download the dataset.
        url = doc.get('permalink')
        url = url.replace('/d/', '/api/views/')
        url += '/rows.tsv?accessType=DOWNLOAD'
        self._download_url = url
        # Optional application token.
        self.app_token = app_token

    def load(self) -> pd.DataFrame:
        """Download the dataset as a pandas data frame.

        Returns
        -------
        pd.DataFrame
        """
        # Get dataset content in tab-delimited format.
        r = _get(self._download_url, app_token=self.app_token)
        # Raise error if request was not successful.
        r.raise_for_status()
        # The response contains a bytes array. Wrap in a BytesIO object to be
        # able to pass the response to pandas.
        return pd.read_csv(BytesIO(r.content), sep='\t')

    def write(self, file: IO):
        """Write the dataset to the given file. The output file format is a
        tab-delimited csv file with the column names as the first line.

        Parameters
        ----------
        file: file object
            File-like object that provides a write method.
        """
        with _get(self._download_url, app_token=self.app_token, stream=True) as r:
            r.raise_for_status()
            for buf in r.iter_content(chunk_size=8192):
                file.write(buf)


class Socrata(Descriptor):
    """Repository handle for the Socrata Open Data API."""
    def __init__(self, app_token: Optional[str] = None):
        """Initialize the data repository descriptor and the optional API
        application token.

        Parameters
        ----------
        app_token: string, default=None
            Optional application token to be included in the header for all API
            requests.
        """
        super(Socrata, self).__init__({
            'id': 'socrata',
            'name': 'Socrata Open Data API',
            'description': 'Open data resources from governments, non-profits, and NGOs around the world'
        })
        self.app_token = app_token

    def catalog(self, domain: Optional[str] = None) -> Iterable[SODADataset]:
        """Generator for a listing of all datasets that are available from the
        repository. Provides to option to filter datasets by their domain.

        Parameters
        ----------
        domain: string, optional=None
            Optional domain name filter for returned datasets.

        Returns
        -------
        iterable of openclean.data.source.socrata.SODADataset
        """
        # Create a list of domains for which datasets are returned.
        domains = self.domains(filter=domain)
        for catalog_url, dom in domains:
            scroll_id = None
            done = False
            while not done:
                url = catalog_url + '?domains={}&only=datasets&limit={}'.format(dom, LIMIT)
                if scroll_id is not None:
                    url += '&scroll_id={}'.format(scroll_id)
                r = _get(url, app_token=self.app_token)
                r.raise_for_status()
                doc = r.json()
                results = doc.get('results', [])
                for obj in results:
                    scroll_id = obj.get('resource').get('id')
                    yield SODADataset(doc=obj, app_token=self.app_token)
                done = len(results) < LIMIT

    def dataset(self, identifier: str) -> SODADataset:
        """Get the handle for the dataset with the given identifier.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.

        Returns
        -------
        openclean.data.source.socrata.SODADataset
        """
        for catalog_url in API_URLS:
            url = '{}?ids={}'.format(catalog_url, identifier)
            r = _get(url, app_token=self.app_token)
            if r.status_code in [200, 202]:
                body = r.json()
                result_size = body.get('resultSetSize')
                if result_size != 1:
                    raise RuntimeError("unexpected result size '{}'".format(result_size))
                return SODADataset(doc=body.get('results')[0], app_token=self.app_token)
        raise ValueError("unknown dataset '{}'".format(identifier))

    def domains(self, filter: Optional[str] = None) -> List[Tuple[str, str]]:
        """Get a list of domain names that are available from the Socrata Open
        Data API. Returns a list of tuples with catalog Url and the domain
        name.

        If the domain filter is given only the domain that matches the filter
        will be returned.

        Returns
        -------
        list of tuples of string and string
        """
        result = list()
        for catalog_url in API_URLS:
            url = catalog_url + '/domains'
            r = _get(url, app_token=self.app_token)
            r.raise_for_status()
            doc = r.json()
            for obj in doc.get('results', []):
                domain = obj['domain']
                if filter is not None and filter == domain:
                    # Return immediately if a domain match is found.
                    return [(catalog_url, domain)]
                result.append((catalog_url, domain))
        return result


# -- Helper functions ---------------------------------------------------------


def _get(
    url: str, app_token: Optional[str] = None, stream: Optional[bool] = None
) -> requests.models.Response:
    """Send GET request to the given Url. Returns the received response.

    Parameters
    ----------
    url: string
        Request Url

    Returns
    -------
    dict
    """
    headers = None
    if app_token is not None:
        headers = {'X-App-Token': app_token}
    return requests.get(url, headers=headers, stream=stream)
