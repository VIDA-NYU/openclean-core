.. _config-ref:

Configuration
=============
openclean defines several environment variables that can be used to configure the behavior of different parts of the package.


Data Storage
------------

Some components of the openclean package store external data files. These files will be stored in sub-folders of a base directory that is specified by the environment variable *OPENCLEAN_DATADIR*. By default, the folder ``openclean/data`` user's cache directory is used as the base directory.


Multi-Threading
---------------

Several tasks in openclean lend themselves well to being run using multiple threads (e.g., key collision clustering using :class:KeyCollision). If the environment variable *OPENCLEAN_THREADS* is set to a positive integer value, it defies the number of parallel treads that are used by default. If the variable is not set (or set to ``1``) a single tread is used.


Configuration for Workers for External Processes
------------------------------------------------

openclean integrates data cleaning and data profiling tools that are implemented in programming languages other than Python and that are executed as external processes. For this purpose, openclean depends on the `flowServ package <https://github.com/scailfin/flowserv-core>`_ that supports execution of sequential workflows (data processing pipelines) in different environments. The environments that are currently supported either use the Python ``subprocess`` package of `Docker <https://www.docker.com>`_.

Workers for external process are configured using configuration files that define the type of execution engine that is used for different tasks (refer to the `flowServ documentation for file formats and configuration options <https://flowserv-core.readthedocs.io/en/latest/source/configuration.html#serial-engine-workers>`_).
