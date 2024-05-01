import datetime
import math
import string
import re

import typing as tp

from . import operations_base as opsb


# ##################################### opsb.Mappers ######################################


class CopyWithDelete(opsb.Mapper):
    def __init__(self, name: str, new_name: str):
        self.name = name
        self.new_name = new_name

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.new_name] = row.pop(self.name)

        yield row


class FilterPunctuation(opsb.Mapper):
    """Left only non-punctuation symbols"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.column] = row[self.column].translate(str.maketrans('', '', string.punctuation))
        yield row


class LowerCase(opsb.Mapper):
    """Replace column value with value in lower case"""
    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(opsb.Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = separator

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        for splitted in Split._split_generator(row[self.column], self.separator):
            new_row = row.copy()
            new_row[self.column] = splitted

            yield new_row

    @staticmethod
    def _split_generator(string_to_split: str, sep: str | None = r"\s+") -> tp.Generator[str, None, None]:
        sep = sep or r'\s+'
        if sep == '':
            return (c for c in string_to_split)

        return (_.group(1) for _ in re.finditer(f'(?:^|{sep})((?:(?!{sep}).)*)', string_to_split))


class Product(opsb.Mapper):
    """Calculates product of multiple columns"""
    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.result_column] = 1
        for column in self.columns:
            row[self.result_column] *= row[column]

        yield row


class FractionLog(opsb.Mapper):
    """Calculates logarithm of fraction of 2 columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'fraction_log') -> None:
        """
        :param columns: column names to make fractionlog
        :param result_column: column name to save result in
        """
        if len(columns) != 2:
            raise ValueError

        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.result_column] = math.log(row[self.columns[0]] / row[self.columns[1]])

        yield row


class Strftime(opsb.Mapper):
    """Calculates string from datetime"""

    def __init__(self, column: tp.Sequence[str], format: str, result_column: str = 'time_str') -> None:
        """
        :param column: datetime column name to make string from
        :param format: arg to take from datetime
        :param result_column: column name to save result in
        """
        if len(column) != 1:
            raise ValueError

        self.column = column[0]
        self.result_column = result_column
        self.format = format

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        if self.format == '%H':
            row[self.result_column] = int(row[self.column].strftime(self.format))
        else:
            row[self.result_column] = row[self.column].strftime(self.format)

        yield row


class Strptime(opsb.Mapper):
    """Calculates datetetime from string"""

    def __init__(self, column: tp.Sequence[str], format: str, result_column: str = 'datetime') -> None:
        """
        :param columns: string column names
        :param format: format for datetime
        :param result_column: column name to save result in
        """
        if len(column) != 1:
            raise ValueError

        self.column = column[0]
        self.format = format
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        str_date = row[self.column]
        if '.%f' in self.format and '.' not in row[self.column]:
            str_date += ('.' + '0' * 6)

        row[self.result_column] = datetime.datetime.strptime(str_date, self.format)

        yield row


class Divide(opsb.Mapper):
    """Calculates fraction of 2 columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'fraction') -> None:
        """
        :param columns: column names to make fraction
        :param result_column: column name to save result in
        """
        if len(columns) != 2:
            raise ValueError

        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.result_column] = row[self.columns[0]] / row[self.columns[1]]

        yield row


class CalcHours(opsb.Mapper):
    """Calculates hours that passed in time between values of 2 columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'hours') -> None:
        """
        :param columns: column names to calculate hours
        :param result_column: column name to save result in
        """
        if len(columns) != 2:
            raise ValueError

        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        row[self.result_column] = (row[self.columns[1]] - row[self.columns[0]]).total_seconds() / 3600

        yield row


class Haversine(opsb.Mapper):
    """Calculates the great circle distance in kilometers between two points on the earth"""
    EARTH_RADIUS_KM = 6373

    def __init__(self, columns: list[str], result_column: str):
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        lon1, lat1, lon2, lat2 = *row[self.columns[0]], *row[self.columns[1]]  # type: ignore
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])  # type: ignore

        # haversine
        a = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        row[self.result_column] = c * self.EARTH_RADIUS_KM

        yield row


class Filter(opsb.Mapper):
    """Remove records that don't satisfy some condition"""
    def __init__(self, condition: tp.Callable[[opsb.TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        if self.condition(row):
            yield row


class Project(opsb.Mapper):
    """Leave only mentioned columns"""
    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: opsb.TRow) -> opsb.TRowsGenerator:
        yield {col: row[col] for col in self.columns}
