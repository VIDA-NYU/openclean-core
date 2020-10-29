====================================
openclean - Data Cleaning for Python
====================================

.. image:: https://img.shields.io/badge/License-BSD-green.svg
    :target: https://github.com/heikomuller/histore/blob/master/LICENSE

.. figure:: https://github.com/VIDA-NYU/openclean-core/blob/master/docs/graphics/logo.png
    :align: center
    :alt: openclean Logo


About
=====

**openclean** is a Python library for data profiling and data cleaning. The is motivated by the fact that data preparation is still a major bottleneck for many data science projects. Data preparation requires profiling to gain an understanding of data quality issues, and data manipulation to transform the data into a form that is fit for the intended purpose.

While a large number of different tools and techniques have previously been developed for profiling and cleaning data, one main issue that we see with these tools is the lack of access to them in a single (unified) framework. Existing tools may be implemented in different programming languages and require significant effort to install and interface with. In other cases, promising data cleaning methods have been published in the scientific literature but there is no suitable codebase available for them. We believe that the lack of seamless access to existing work is a major contributor to why data preparation is so time consuming.

The goal of **openclean** goal is to bring together data cleaning tools in a single environment that is easy and intuitive to use for a data scientist. **openclean** allows users to compose and execute cleaning pipelines that are built using a variety of different tools. We aim for **openclean** to be flexible and extensible to allow easy integration of new functionality. To this end, we define a set of primitives and API’s for the different types of operators (actors) in **openclean** pipelines.



Installation
============

Install **openclean** from the  `Python Package Index (PyPI) <https://pypi.org/>`_ using ``pip`` with:

```
pip install opencean-core
```



Examples
========

We include several example notebooks in this repository that demonstrate possible use cases for **openclean**.


New York City Restaurant Inspection Results
-------------------------------------------

In this example our goal is to reproduce a previous `study from 2014 that looks at the distribution of restaurant inspection grades in New York City <https://iquantny.tumblr.com/post/76928412519/think-nyc-restaurant-grading-is-flawed-heres>`_`. For our study, we use data that was downloaded in Sept. 2019. The example is split into three different Jupyter notebooks:

- `Download the Dataset <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/NYCRestaurantInspections/NYC%20Restaurant%20Inspections%20-%20Download.ipynb>`_
- `Data Profiling <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/NYCRestaurantInspections/NYC%20Restaurant%20Inspections%20-%20Profiling.ipynb>`_
- `Data Cleaning <https://github.com/VIDA-NYU/openclean-core/blob/master/examples/notebooks/NYCRestaurantInspections/NYC%20Restaurant%20Inspections%20-%20Cleaning.ipynb>`_
