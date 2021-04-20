from typing import Optional

from openclean.data.stream.base import DataRow
from openclean.data.types import DatasetSchema
from openclean.function.matching.base import StringMatcher
from openclean.data.mapping import Mapping
from openclean.operator.stream.consumer import StreamConsumer
from openclean.operator.stream.processor import StreamProcessor


class BestMatches(StreamConsumer, StreamProcessor):
    def __init__(
        self, matcher: StringMatcher, include_vocab: Optional[bool] = False,
        mapping: Optional[Mapping] = None
    ):
        """Initialize the different components of the bast matches operator. If
        the operator is opened as a consumer the map argument will not be None.

        Parameters
        ----------
        matcher: openclean.function.matching.base.VocabularyMatcher
            Matcher to compute matches for the terms in a controlled vocabulary.
        include_vocab: bool, default=False
            If this flag is False the resulting mapping will only contain matches
            for terms that are not in the vocabulary that is associated with the
            given matcher.
        mapping: openclean.data.mapping.Mapping, default=None
            Mapping instance that is used to collect the matches.
        """
        self.matcher = matcher
        self.include_vocab = include_vocab
        self.mapping = mapping

    def close(self) -> Mapping:
        """Return the collected mapping at the end of the stream.

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        return self.mapping

    def consume(self, rowid: int, row: DataRow) -> DataRow:
        """Consume the given row. Assumes that the row contains exactly one
        column.

        Parameters
        ----------
        values: iterable of strings
            List of terms (e.g., from a data frame column) for which matches are
            computed for the returned mapping.
        matcher: openclean.function.matching.base.VocabularyMatcher
            Matcher to compute matches for the terms in a controlled vocabulary.
        include_vocab: bool, default=False
            If this flag is False the resulting mapping will only contain matches
            for terms that are not in the vocabulary that is associated with the
            given matcher.

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        val = row[0]
        if self.include_vocab or val not in self.matcher.vocabulary:
            self.mapping.add(val, self.matcher.find_matches(val))

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        best matches consumer.

        Raises a ValueError if the given schema contains more than one column.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer

        Raises
        ------
        ValueError
        """
        # Raise an error if the row schema contains more than one column.
        if len(schema) != 1:
            raise ValueError('cannot match rows with {} columns'.format(len(schema)))
        return BestMatches(
            matcher=self.matcher,
            include_vocab=self.include_vocab,
            mapping=Mapping()
        )
