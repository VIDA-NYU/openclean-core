# openclean - Data Cleaning for Python - Changelog

### 0.1.0 - 2021-02-23

* Initial release.


### 0.1.1 - 2021-03-02

* Bump `flowserv-core` dependency to version `0.8.0` for better support of running applications that are not implemented in Python.
* Introduce environment variable *OPENCLEAN_WORKERS* to configure workers for `flowserv-core`.
* Replace environment variables *OPENCLEAN_METADATA_DIR* with *OPENCLEAN_DATADIR*.


### 0.2.0 - 2021-03-17

* Standardize parameter names for sample methods (\#115)
* Bug fix for `openclean-notebook`


### 0.3.0 - 2021-03-29

* Add `openclean.function.token.base.Token` as separate class.
* Rename `openclean.function.token.base.StringTokenizer` to `Tokenizer`
* Adjust token transformer and tokenizer for new Token class.
* Change structure of datatype count in column profiler.
* Option to get set of conflicting values from `DataFrameGrouping` groups.
* Multi-threading for `ValueFunction.apply()`.
* Separate DBSCAN outlier class.
* Move us-street name functions to `openclean-geo`.


### 0.3.1 - 2021-03-30

* Add optional version parameter when requesting metadata for a dataset version in `openclean.engine.dataset.DatasetHandle`.


### 0.3.2 - 2021-04-06

* Make checking out a committed dataset in the `openclean.data.archive.base.ArchiveStore` optional.
* Enable cache refresh for cached datasets in `openclean.data.archive.cache.CachedDatastore`.


### 0.4.0 - 2021-04-27

* Use compact serialization for HISTORE archives.
* Load and sample datasets from a data stream in `openclean.engine.base.OpencleanEngine`.
* Support stream operators on dataset snapshots in  `openclean.engine.base.OpencleanEngine`.
* Add summary for data frame conflict groups.
