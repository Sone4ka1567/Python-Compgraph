import itertools

import typing as tp

from . import operations_base as opsb


# ##################################### Joiners ######################################


class InnerJoiner(opsb.Joiner):
    """Join with inner strategy"""

    def __call__(
        self, keys: tp.Sequence[str],
        rows_a: opsb.TRowsIterable, rows_b: opsb.TRowsIterable
    ) -> opsb.TRowsGenerator:
        yield from self._inner_join(keys, rows_a, rows_b)


class OuterJoiner(opsb.Joiner):
    """Join with outer strategy"""

    def __call__(
        self, keys: tp.Sequence[str],
        rows_a: opsb.TRowsIterable, rows_b: opsb.TRowsIterable
    ) -> opsb.TRowsGenerator:
        rows_a = iter(rows_a)
        next_ = next(rows_a, None)

        if next_ is None:
            yield from rows_b
            return
        else:
            rows_a = itertools.chain([next_], rows_a)

        rows_b = iter(rows_b)
        next_ = next(rows_b, None)

        if next_ is None:
            yield from rows_a
            return
        else:
            rows_b = itertools.chain([next_], rows_b)

        yield from self._inner_join(keys, rows_a, rows_b)


class LeftJoiner(opsb.Joiner):
    """Join with left strategy"""

    def __call__(
        self, keys: tp.Sequence[str], rows_a: opsb.TRowsIterable,
        rows_b: opsb.TRowsIterable
    ) -> opsb.TRowsGenerator:
        rows_b = iter(rows_b)
        next_ = next(rows_b, None)

        if next_ is None:
            yield from rows_a
            return
        else:
            rows_b = itertools.chain([next_], rows_b)

        yield from self._inner_join(keys, rows_a, rows_b)


class RightJoiner(opsb.Joiner):
    """Join with right strategy"""

    def __call__(
        self, keys: tp.Sequence[str],
        rows_a: opsb.TRowsIterable, rows_b: opsb.TRowsIterable
    ) -> opsb.TRowsGenerator:
        rows_a = iter(rows_a)
        next_ = next(rows_a, None)

        if next_ is None:
            yield from rows_b
            return
        else:
            rows_a = itertools.chain([next_], rows_a)

        yield from self._inner_join(keys, rows_a, rows_b)
