.. image:: graphics/logo.png
    :width: 350px
    :align: center
    :height: 80px
    :alt: openclean-logo
|

Welcome to openclean's Documentation!
=====================================

openclean is a Python library for data profiling and data cleaning. The project is motivated by the fact that data preparation is still a major bottleneck for many data science projects. Data preparation requires profiling to gain an understanding of data quality issues, and data manipulation to transform the data into a form that is fit for the intended purpose.

While a large number of different tools and techniques have previously been developed for profiling and cleaning data, one main issue that we see with these tools is the lack of access to them in a single (unified) framework. Existing tools may be implemented in different programming languages and require significant effort to install and interface with. In other cases, promising data cleaning methods have been published in the scientific literature but there is no suitable codebase available for them. We believe that the lack of seamless access to existing work is a major contributor to why data preparation is so time consuming.

The goal of openclean goal is to bring together data cleaning tools in a single environment that is easy and intuitive to use for a data scientist. openclean allows users to compose and execute cleaning pipelines that are built using a variety of different tools. We aim for openclean to be flexible and extensible to allow easy integration of new functionality. To this end, we define a set of primitives and API’s for the different types of operators (actors) in openclean pipelines.


..  toctree::
    :maxdepth: 2
    :caption: Contents:

    source/installation
    source/start
    source/datamodel
    source/profile
    source/transform
    source/clean
    source/enrich
    source/provenance
    source/examples
    source/extras
    source/configuration
    source/contribute
    source/faq

.. _api-ref:

..  toctree::
    :maxdepth: 1
    :caption: API Reference:

    source/api/modules
