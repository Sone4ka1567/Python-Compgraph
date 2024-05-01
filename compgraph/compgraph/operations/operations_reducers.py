
import heapq

from collections import defaultdict

import typing as tp

from . import operations_base as opsb

# ##################################### opsb.Reducers ######################################


class TopN(opsb.Reducer):
    """Calculate top N by value"""
    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tuple[str, ...], rows: opsb.TRowsIterable) -> opsb.TRowsGenerator:
        for row in heapq.nlargest(self.n, rows, lambda r: r[self.column_max]):
            yield row


class TermFrequency(opsb.Reducer):
    """Calculate frequency of values in column"""
    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: opsb.TRowsIterable) -> opsb.TRowsGenerator:
        counts: dict[str, int] = defaultdict(int)
        values: tp.Any = dict()
        count_rows = 0

        for row in rows:
            count_rows += 1
            counts[row[self.words_column]] += 1

            values.update((col, row[col]) for col in group_key)

        for word, count in counts.items():
            yield dict(values, **{self.words_column: word, self.result_column: count / count_rows})


class Count(opsb.Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: opsb.TRowsIterable) -> opsb.TRowsGenerator:
        count_rows = 0
        values: tp.Any = dict()

        for row in rows:
            values.update((col, row[col]) for col in group_key)
            count_rows += 1

        yield dict(values, **{self.column: count_rows})


class Sum(opsb.Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """
    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: opsb.TRowsIterable) -> opsb.TRowsGenerator:
        sum = 0
        values: tp.Any = dict()

        for row in rows:
            values.update((col, row[col]) for col in group_key)
            sum += row[self.column]

        yield dict(values, **{self.column: sum})
