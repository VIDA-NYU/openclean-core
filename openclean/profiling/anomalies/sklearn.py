# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic outlier detector that uses scikit-learn outlier detection or
clustering algorithms.
"""

from collections import Counter
from sklearn.base import BaseEstimator
from typing import List, Optional

from openclean.embedding.base import Embedding, ValueEmbedder
from openclean.embedding.feature.default import StandardEmbedding
from openclean.profiling.anomalies.base import AnomalyDetector


class SklearnOutliers(AnomalyDetector):
    """Detect outliers in a given value stream based on a scikit-learn outlier
    detection or clustering algoritm. Expects a scikit-learn estimator and a
    value embedding generator. Outlier detection uses the fit_predict method of
    the scikit-learn estimator to get labels for each value. Values that are
    assigned a label of -1 are considered outliers.
    """
    def __init__(
        self, algorithm: BaseEstimator,
        features: Optional[ValueEmbedder] = None
    ):
        """Initialize the embedding generator and the outlier detection or
        clustering algorithm. If no feature generator is given the default
        feature generator is used.

        Parameters
        ----------
        algorithm: sklearn.base.BaseEstimator
            Algorithm that is used to detect outliers in a data stream.
        features: openclean.profiling.embedding.base.ValueEmbedder, optional
            Feature vector generator for values in a data stream.
        """
        self.algorithm = algorithm
        self.features = StandardEmbedding() if features is None else features

    def process(self, values: Counter) -> List:
        """Return set of values that are identified as outliers. This anomaly
        detector does not provide any additional provenance for the detected
        outlier values (other than the name of the used algorithm).

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        # Get the vector embedding for all values in the data stream
        vec = Embedding(features=self.features).exec(values)
        # Get labels using the fit_predict() metod of the estimator
        labels = self.algorithm.fit_predict(vec.data)
        # Return values that were assigned label -1.
        result = list()
        keys = list(vec.keys())
        for i in range(len(keys)):
            if labels[i] == -1:
                result.append(keys[i])
        return result


class DBSCANOutliers(SklearnOutliers):
    """Perform outlier detection using DBSCAN clustering."""
    def __init__(
        self, features=None, eps=0.5, min_samples=5, metric='minkowski',
        metric_params=None, algorithm='auto', leaf_size=30, p=2, n_jobs=None
    ):
        """Initialize the feature generator and all parameters of the DBSCAN
        implementation in scikit-learn (documentation copied below).

        Parameters
        ----------
        features: openclean.profiling.embedding.base.ValueEmbedder, optional
            Generator for feature vectors that computes a vector of numeric values
            for a given scalar value (or tuple).
        eps : float, default=0.5
            The maximum distance between two samples for one to be considered
            as in the neighborhood of the other. This is not a maximum bound
            on the distances of points within a cluster. This is the most
            important DBSCAN parameter to choose appropriately for your data set
            and distance function.
        min_samples : int, default=5
            The number of samples (or total weight) in a neighborhood for a point
            to be considered as a core point. This includes the point itself.
        metric : string, or callable
            The metric to use when calculating distance between instances in a
            feature array. If metric is a string or callable, it must be one of
            the options allowed by :func:`sklearn.metrics.pairwise_distances` for
            its metric parameter.
            If metric is "precomputed", X is assumed to be a distance matrix and
            must be square during fit.
            X may be a :term:`sparse graph <sparse graph>`,
            in which case only "nonzero" elements may be considered neighbors.
        metric_params : dict, default=None
            Additional keyword arguments for the metric function.
        algorithm : {'auto', 'ball_tree', 'kd_tree', 'brute'}, default='auto'
            The algorithm to be used by the NearestNeighbors module
            to compute pointwise distances and find nearest neighbors.
            See NearestNeighbors module documentation for details.
        leaf_size : int, default=30
            Leaf size passed to BallTree or cKDTree. This can affect the speed
            of the construction and query, as well as the memory required
            to store the tree. The optimal value depends
            on the nature of the problem.
        p : float, default=2
            The power of the Minkowski metric to be used to calculate distance
            between points.
        n_jobs : int, default=None
            The number of parallel jobs to run for neighbors search. ``None`` means
            1 unless in a :obj:`joblib.parallel_backend` context. ``-1`` means
            using all processors. See :term:`Glossary <n_jobs>` for more details.
            If precomputed distance are used, parallel execution is not available
            and thus n_jobs will have no effect.
        """
        # Initialize the DBSCAN estimator
        from sklearn.cluster import DBSCAN
        algo = DBSCAN(
            eps=eps,
            min_samples=min_samples,
            metric=metric,
            metric_params=metric_params,
            algorithm=algorithm,
            leaf_size=leaf_size,
            p=p,
            n_jobs=n_jobs
        )
        super(DBSCANOutliers, self).__init__(algorithm=algo, features=features)


# -- Functions for specific scikit-learn outlier detectors --------------------

def dbscan(
    df, columns, features=None, eps=0.5, min_samples=5, metric='minkowski',
    metric_params=None, algorithm='auto', leaf_size=30, p=2, n_jobs=None
):
    """Perform outlier detection using DBSCAN clustering. Returns values as
    outliers that are not added to any cluster. Supports all parameters of
    the DBSCAN implementation in scikit-learn (documentation copied below).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    features: openclean.profiling.embedding.base.ValueEmbedder, optional
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).
    eps : float, default=0.5
        The maximum distance between two samples for one to be considered
        as in the neighborhood of the other. This is not a maximum bound
        on the distances of points within a cluster. This is the most
        important DBSCAN parameter to choose appropriately for your data set
        and distance function.
    min_samples : int, default=5
        The number of samples (or total weight) in a neighborhood for a point
        to be considered as a core point. This includes the point itself.
    metric : string, or callable
        The metric to use when calculating distance between instances in a
        feature array. If metric is a string or callable, it must be one of
        the options allowed by :func:`sklearn.metrics.pairwise_distances` for
        its metric parameter.
        If metric is "precomputed", X is assumed to be a distance matrix and
        must be square during fit.
        X may be a :term:`sparse graph <sparse graph>`,
        in which case only "nonzero" elements may be considered neighbors.
    metric_params : dict, default=None
        Additional keyword arguments for the metric function.
    algorithm : {'auto', 'ball_tree', 'kd_tree', 'brute'}, default='auto'
        The algorithm to be used by the NearestNeighbors module
        to compute pointwise distances and find nearest neighbors.
        See NearestNeighbors module documentation for details.
    leaf_size : int, default=30
        Leaf size passed to BallTree or cKDTree. This can affect the speed
        of the construction and query, as well as the memory required
        to store the tree. The optimal value depends
        on the nature of the problem.
    p : float, default=2
        The power of the Minkowski metric to be used to calculate distance
        between points.
    n_jobs : int, default=None
        The number of parallel jobs to run for neighbors search. ``None`` means
        1 unless in a :obj:`joblib.parallel_backend` context. ``-1`` means
        using all processors. See :term:`Glossary <n_jobs>` for more details.
        If precomputed distance are used, parallel execution is not available
        and thus n_jobs will have no effect.

    Returns
    -------
    list
    """
    # Run the scikit-learn outlier detection algoritm with DBSCAN as the
    # estimator.
    op = DBSCANOutliers(
        features=features,
        eps=eps,
        min_samples=min_samples,
        metric=metric,
        metric_params=metric_params,
        algorithm=algorithm,
        leaf_size=leaf_size,
        p=p,
        n_jobs=n_jobs
    )
    return op.run(df=df, columns=columns)


def isolation_forest(
    df, columns, features=None, n_estimators=100, max_samples='auto',
    contamination='auto', max_features=1., bootstrap=False, n_jobs=None,
    random_state=None, verbose=0, warm_start=False
):
    """Perform outlier detection using the isolation forest outlier detection.
    Supports most parameters of the IsolationForest implementation in
    scikit-learn (documentation copied below).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    features: openclean.profiling.embedding.base.ValueEmbedder, optional
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).
    n_estimators : int, default=100
        The number of base estimators in the ensemble.
    max_samples : "auto", int or float, default="auto"
        The number of samples to draw from X to train each base estimator.
            - If int, then draw `max_samples` samples.
            - If float, then draw `max_samples * X.shape[0]` samples.
            - If "auto", then `max_samples=min(256, n_samples)`.
        If max_samples is larger than the number of samples provided,
        all samples will be used for all trees (no sampling).
    contamination : 'auto' or float, default='auto'
        The amount of contamination of the data set, i.e. the proportion
        of outliers in the data set. Used when fitting to define the threshold
        on the scores of the samples.
            - If 'auto', the threshold is determined as in the
              original paper.
            - If float, the contamination should be in the range [0, 0.5].
        .. versionchanged:: 0.22
           The default value of ``contamination`` changed from 0.1
           to ``'auto'``.
    max_features : int or float, default=1.0
        The number of features to draw from X to train each base estimator.
            - If int, then draw `max_features` features.
            - If float, then draw `max_features * X.shape[1]` features.
    bootstrap : bool, default=False
        If True, individual trees are fit on random subsets of the training
        data sampled with replacement. If False, sampling without replacement
        is performed.
    n_jobs : int, default=None
        The number of jobs to run in parallel for both :meth:`fit` and
        :meth:`predict`. ``None`` means 1 unless in a
        :obj:`joblib.parallel_backend` context. ``-1`` means using all
        processors. See :term:`Glossary <n_jobs>` for more details.
    random_state : int or RandomState, default=None
        Controls the pseudo-randomness of the selection of the feature
        and split values for each branching step and each tree in the forest.
        Pass an int for reproducible results across multiple function calls.
        See :term:`Glossary <random_state>`.
    verbose : int, default=0
        Controls the verbosity of the tree building process.
    warm_start : bool, default=False
        When set to ``True``, reuse the solution of the previous call to fit
        and add more estimators to the ensemble, otherwise, just fit a whole
        new forest. See :term:`the Glossary <warm_start>`.

    Returns
    -------
    list
    """
    # Initialize the IsolationForest estimator
    from sklearn.ensemble import IsolationForest
    algo = IsolationForest(
        n_estimators=n_estimators,
        max_samples=max_samples,
        contamination=contamination,
        max_features=max_features,
        bootstrap=bootstrap,
        n_jobs=n_jobs,
        random_state=random_state,
        verbose=verbose,
        warm_start=warm_start
    )
    # Run the scikit-learn outlier detection algoritm with IsolationForest as
    # the estimator.
    op = SklearnOutliers(algorithm=algo, features=features)
    return op.run(df=df, columns=columns)


def local_outlier_factor(
    df, columns, features=None, n_neighbors=20, algorithm='auto', leaf_size=30,
    metric='minkowski', p=2, metric_params=None, contamination='auto',
    novelty=False, n_jobs=None
):
    """Perform outlier detection using Local Outlier Factor (LOF). Supports all
    parameters of the LocalOutlierFactor implementation in scikit-learn
    (documentation copied below).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    features: openclean.profiling.embedding.base.ValueEmbedder
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).
    n_neighbors : int, default=20
        Number of neighbors to use by default for :meth:`kneighbors` queries.
        If n_neighbors is larger than the number of samples provided,
        all samples will be used.
    algorithm : {'auto', 'ball_tree', 'kd_tree', 'brute'}, default='auto'
        Algorithm used to compute the nearest neighbors:
        - 'ball_tree' will use :class:`BallTree`
        - 'kd_tree' will use :class:`KDTree`
        - 'brute' will use a brute-force search.
        - 'auto' will attempt to decide the most appropriate algorithm
          based on the values passed to :meth:`fit` method.
        Note: fitting on sparse input will override the setting of
        this parameter, using brute force.
    leaf_size : int, default=30
        Leaf size passed to :class:`BallTree` or :class:`KDTree`. This can
        affect the speed of the construction and query, as well as the memory
        required to store the tree. The optimal value depends on the
        nature of the problem.
    metric : str or callable, default='minkowski'
        metric used for the distance computation. Any metric from scikit-learn
        or scipy.spatial.distance can be used.
        If metric is "precomputed", X is assumed to be a distance matrix and
        must be square. X may be a sparse matrix, in which case only "nonzero"
        elements may be considered neighbors.
        If metric is a callable function, it is called on each
        pair of instances (rows) and the resulting value recorded. The callable
        should take two arrays as input and return one value indicating the
        distance between them. This works for Scipy's metrics, but is less
        efficient than passing the metric name as a string.
        Valid values for metric are:
        - from scikit-learn: ['cityblock', 'cosine', 'euclidean', 'l1', 'l2',
          'manhattan']
        - from scipy.spatial.distance: ['braycurtis', 'canberra', 'chebyshev',
          'correlation', 'dice', 'hamming', 'jaccard', 'kulsinski',
          'mahalanobis', 'minkowski', 'rogerstanimoto', 'russellrao',
          'seuclidean', 'sokalmichener', 'sokalsneath', 'sqeuclidean',
          'yule']
        See the documentation for scipy.spatial.distance for details on these
        metrics:
        https://docs.scipy.org/doc/scipy/reference/spatial.distance.html
    p : int, default=2
        Parameter for the Minkowski metric from
        :func:`sklearn.metrics.pairwise.pairwise_distances`. When p = 1, this
        is equivalent to using manhattan_distance (l1), and euclidean_distance
        (l2) for p = 2. For arbitrary p, minkowski_distance (l_p) is used.
    metric_params : dict, default=None
        Additional keyword arguments for the metric function.
    contamination : 'auto' or float, default='auto'
        The amount of contamination of the data set, i.e. the proportion
        of outliers in the data set. When fitting this is used to define the
        threshold on the scores of the samples.
        - if 'auto', the threshold is determined as in the
          original paper,
        - if a float, the contamination should be in the range [0, 0.5].
    novelty : bool, default=False
        By default, LocalOutlierFactor is only meant to be used for outlier
        detection (novelty=False). Set novelty to True if you want to use
        LocalOutlierFactor for novelty detection. In this case be aware that
        that you should only use predict, decision_function and score_samples
        on new unseen data and not on the training set.
    n_jobs : int, default=None
        The number of parallel jobs to run for neighbors search.
        ``None`` means 1 unless in a :obj:`joblib.parallel_backend` context.
        ``-1`` means using all processors. See :term:`Glossary <n_jobs>`
        for more details.

    Returns
    -------
    list
    """
    # Initialize the LocalOutlierFactor estimator.
    from sklearn.neighbors import LocalOutlierFactor
    algo = LocalOutlierFactor(
        n_neighbors=n_neighbors,
        algorithm=algorithm,
        leaf_size=leaf_size,
        metric=metric,
        p=p,
        metric_params=metric_params,
        contamination=contamination,
        novelty=novelty,
        n_jobs=n_jobs
    )
    # Run the scikit-learn outlier detection algoritm with LocalOutlierFactor
    # as the estimator.
    op = SklearnOutliers(algorithm=algo, features=features)
    return op.run(df=df, columns=columns)


def one_class_svm(
    df, columns, features=None, kernel='rbf', degree=3, gamma='scale',
    coef0=0.0, tol=1e-3, nu=0.5, shrinking=True, cache_size=200, verbose=False,
    max_iter=-1
):
    """Perform outlier detection using Unsupervised Outlier Detection. Supports
    all parameters of the OneClassSVM implementation in scikit-learn
    (documentation copied below).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    features: openclean.profiling.embedding.base.ValueEmbedder
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).
    kernel : {'linear', 'poly', 'rbf', 'sigmoid', 'precomputed'}, default='rbf'
         Specifies the kernel type to be used in the algorithm.
         It must be one of 'linear', 'poly', 'rbf', 'sigmoid', 'precomputed' or
         a callable.
         If none is given, 'rbf' will be used. If a callable is given it is
         used to precompute the kernel matrix.
    degree : int, default=3
        Degree of the polynomial kernel function ('poly').
        Ignored by all other kernels.
    gamma : {'scale', 'auto'} or float, default='scale'
        Kernel coefficient for 'rbf', 'poly' and 'sigmoid'.
        - if ``gamma='scale'`` (default) is passed then it uses
          1 / (n_features * X.var()) as value of gamma,
        - if 'auto', uses 1 / n_features.
        .. versionchanged:: 0.22
           The default value of ``gamma`` changed from 'auto' to 'scale'.
    coef0 : float, default=0.0
        Independent term in kernel function.
        It is only significant in 'poly' and 'sigmoid'.
    tol : float, default=1e-3
        Tolerance for stopping criterion.
    nu : float, default=0.5
        An upper bound on the fraction of training
        errors and a lower bound of the fraction of support
        vectors. Should be in the interval (0, 1]. By default 0.5
        will be taken.
    shrinking : bool, default=True
        Whether to use the shrinking heuristic.
        See the :ref:`User Guide <shrinking_svm>`.
    cache_size : float, default=200
        Specify the size of the kernel cache (in MB).
    verbose : bool, default=False
        Enable verbose output. Note that this setting takes advantage of a
        per-process runtime setting in libsvm that, if enabled, may not work
        properly in a multithreaded context.
    max_iter : int, default=-1
        Hard limit on iterations within solver, or -1 for no limit.

    Returns
    -------
    list
    """
    # Initialize the OneClassSVM estimator.
    from sklearn.svm import OneClassSVM
    algo = OneClassSVM(
        kernel=kernel,
        degree=degree,
        gamma=gamma,
        coef0=coef0,
        tol=tol,
        nu=nu,
        shrinking=shrinking,
        cache_size=cache_size,
        verbose=verbose,
        max_iter=max_iter
    )
    # Run the scikit-learn outlier detection algoritm with OneClassSVM
    # as the estimator.
    op = SklearnOutliers(algorithm=algo, features=features)
    return op.run(df=df, columns=columns)


def robust_covariance(
    df, columns, features=None, store_precision=True, assume_centered=False,
    support_fraction=None, contamination=0.1, random_state=None
):
    """Perform outlier detection using EllipticEnvelope for detecting outliers
    in a Gaussian distributed dataset. Supports all parameters of the
    EllipticEnvelope implementation in scikit-learn (documentation copied
    below).

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    features: openclean.profiling.embedding.base.ValueEmbedder
        Generator for feature vectors that computes a vector of numeric values
        for a given scalar value (or tuple).
    store_precision : bool, default=True
        Specify if the estimated precision is stored.
    assume_centered : bool, default=False
        If True, the support of robust location and covariance estimates
        is computed, and a covariance estimate is recomputed from it,
        without centering the data.
        Useful to work with data whose mean is significantly equal to
        zero but is not exactly zero.
        If False, the robust location and covariance are directly computed
        with the FastMCD algorithm without additional treatment.
    support_fraction : float, default=None
        The proportion of points to be included in the support of the raw
        MCD estimate. If None, the minimum value of support_fraction will
        be used within the algorithm: `[n_sample + n_features + 1] / 2`.
        Range is (0, 1).
    contamination : float, default=0.1
        The amount of contamination of the data set, i.e. the proportion
        of outliers in the data set. Range is (0, 0.5).
    random_state : int or RandomState instance, default=None
        Determines the pseudo random number generator for shuffling
        the data. Pass an int for reproducible results across multiple function
        calls. See :term: `Glossary <random_state>`.

    Returns
    -------
    list
    """
    # Initialize the EllipticEnvelope estimator.
    from sklearn.covariance import EllipticEnvelope
    algo = EllipticEnvelope(
        store_precision=store_precision,
        assume_centered=assume_centered,
        support_fraction=support_fraction,
        contamination=contamination,
        random_state=random_state
    )
    # Run the scikit-learn outlier detection algoritm with EllipticEnvelope
    # as the estimator.
    op = SklearnOutliers(algorithm=algo, features=features)
    return op.run(df=df, columns=columns)
