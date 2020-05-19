# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for repository downloader."""

from abc import ABCMeta, abstractmethod


class RepositoryDownloader(metaclass=ABCMeta):
    """Base class for masterdata repositories. Defines methods for listing the
    datasets that are available for download and for dataset download.
    """
    @abstractmethod
    def datasets(self):
        """Get a description of datasets that are available for download from
        the repository. Returns a list of dataset descriptors.

        Returns
        -------
        list(openclean.data.downloader.dataset.DatasetDescriptor)
        """
        raise NotImplementedError()

    @abstractmethod
    def download(self, datasets=None, properties=None):
        """Download one or more datasets from a masterdata repository. Returns
        a dictionary that maps the identifier of the downloaded datasets to
        data frames containing the data.

        Parameters
        ----------
        datasets: string or list of string, default=None
            List of identifier for datasets that are downladed. The set of
            valid identifier is depending on the repository.
        properties: dict, default=None
            Additional properties for the download. The set of valid properties
            is dependent on the repository.

        Returns
        -------
        dict

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()
