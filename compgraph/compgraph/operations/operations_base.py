import itertools

from abc import abstractmethod, ABC
import typing as tp

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Read(Operation):
    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row


# ##################################### Operations ######################################


class Mapper(ABC):
    """Base class for mappers"""
    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""
    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for _, group_items in itertools.groupby(rows, lambda row: {column: row[column] for column in self.keys}):
            yield from self.reducer(tuple(self.keys), group_items)


class Joiner(ABC):
    """Base class for joiners"""
    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        self.suffix_a = suffix_a
        self.suffix_b = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def _inner_join(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows = list(rows_b)

        for row_a in rows_a:
            for row_b in rows:
                new_row = {key: row_a[key] for key in keys}

                a_cols = set(row_a.keys()) - set(keys)
                b_cols = set(row_b.keys()) - set(keys)

                for column in (a_cols - b_cols):
                    new_row[column] = row_a[column]

                for column in (b_cols - a_cols):
                    new_row[column] = row_b[column]

                for column in (a_cols & b_cols):
                    new_row[column + self.suffix_a] = row_a[column]
                    new_row[column + self.suffix_b] = row_b[column]

                yield new_row


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        self.keys = keys
        self.joiner = joiner

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        groups_left = iter(itertools.groupby(
            rows, key=lambda row: tuple(row[column] for column in self.keys)
        ))
        groups_right = iter(itertools.groupby(
            args[0], key=lambda row: tuple(row[column] for column in self.keys)
        ))

        key_left, group_left = next(groups_left, (None, []))
        key_right, group_right = next(groups_right, (None, []))

        while True:
            if key_left is None and key_right is None:
                break

            elif key_left is None or (key_right is not None and key_right < key_left):
                yield from self.joiner(self.keys, [], group_right)

                key_right, group_right = next(groups_right, (None, []))
            elif key_right is None or (key_left is not None and key_left < key_right):
                yield from self.joiner(self.keys, group_left, [])

                key_left, group_left = next(groups_left, (None, []))
            elif key_left == key_right:
                yield from self.joiner(self.keys, group_left, group_right)

                key_left, group_left = next(groups_left, (None, []))
                key_right, group_right = next(groups_right, (None, []))


# ##################################### Dummy operator ######################################


class DummyMapper(Mapper):
    """Yield exactly the row passed"""
    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break
