import copy
import dataclasses
import pytest
import typing as tp

from datetime import datetime
from pytest import approx

from compgraph import operations as ops


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = args

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    expected: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    mapper_item: int = 0
    mapper_expected_items: tuple[int, ...] = (0,)


MAP_CASES = [
    MapCase(
        mapper=ops.CopyWithDelete(name='before', new_name='after'),
        data=[
            {'before': 1},
            {'before': 5},
            {'before': 6},
            {'before': 9},
        ],
        expected=[
            {'after': 1},
            {'after': 5},
            {'after': 6},
            {'after': 9},
        ],
        cmp_keys=('before', 'after')
    ),
    MapCase(
        mapper=ops.FractionLog(columns=['x', 'y']),
        data=[
            {'x': 2, 'y': 3},
            {'x': 10, 'y': 4},
            {'x': 0.777, 'y': 0.07},
        ],
        expected=[
            {'x': 2, 'y': 3, 'fraction_log': approx(-0.40546, 0.001)},
            {'x': 10, 'y': 4, 'fraction_log': approx(0.916290, 0.001)},
            {'x': 0.777, 'y': 0.07, 'fraction_log': approx(2.406945, 0.001)},
        ],
        cmp_keys=('x', 'y', 'fraction_log')
    ),
    MapCase(
        mapper=ops.Strftime(column=['column'], format='%Y-%m-%d %H:%M:%S'),
        data=[
            {'column': datetime(2022, 10, 20, 15, 35, 30)},
            {'column': datetime(2002, 5, 24, 1, 34, 20)},
            {'column': datetime(2002, 8, 16, 16, 16, 16)},
        ],
        expected=[
            {'column': datetime(2022, 10, 20, 15, 35, 30), 'time_str': '2022-10-20 15:35:30'},
            {'column': datetime(2002, 5, 24, 1, 34, 20), 'time_str': '2002-05-24 01:34:20'},
            {'column': datetime(2002, 8, 16, 16, 16, 16), 'time_str': '2002-08-16 16:16:16'},
        ],
        cmp_keys=('column', 'time_str')
    ),
    MapCase(
        mapper=ops.Divide(columns=['first', 'second']),
        data=[
            {'first': 10, 'second': 20},
            {'first': 5, 'second': 3},
            {'first': 3.5, 'second': 1.23},
            {'first': 0.12, 'second': 0.5},
        ],
        expected=[
            {'first': 10, 'second': 20, 'fraction': approx(0.5, abs=0.001)},
            {'first': 5, 'second': 3, 'fraction': approx(1.666, abs=0.001)},
            {'first': 3.5, 'second': 1.23, 'fraction': approx(2.845, abs=0.001)},
            {'first': 0.12, 'second': 0.5, 'fraction': approx(0.24, abs=0.01)},
        ],
        cmp_keys=('first', 'second', 'fraction')
    ),
    MapCase(
        mapper=ops.Haversine(columns=['start', 'end'], result_column='result'),
        data=[
            {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953]},
            {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824]},
            {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356]},
            {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035]}
        ],
        expected=[
            {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
             'result': approx(0.032, abs=0.001)},
            {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
             'result': approx(0.045, abs=0.001)},
            {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356],
             'result': approx(0.036, abs=0.001)},
            {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035],
             'result': approx(0.041, abs=0.001)}
        ],
        cmp_keys=('first', 'second', 'result')
    ),
    MapCase(
        mapper=ops.Strptime(column=['column'], format='%Y-%m-%d %H:%M:%S'),
        data=[
            {'column': '2022-10-20 15:35:30'},
            {'column': '2002-05-24 01:34:20'},
            {'column': '2002-08-16 16:16:16'},
        ],
        expected=[
            {'column': '2022-10-20 15:35:30', 'datetime': datetime(2022, 10, 20, 15, 35, 30)},
            {'column': '2002-05-24 01:34:20', 'datetime': datetime(2002, 5, 24, 1, 34, 20)},
            {'column': '2002-08-16 16:16:16', 'datetime': datetime(2002, 8, 16, 16, 16, 16)},
        ],
        cmp_keys=('column', 'datetime')
    ),
    MapCase(
        mapper=ops.CalcHours(columns=['start', 'end']),
        data=[
            {'start': datetime(2022, 10, 20, 15, 35, 30), 'end': datetime(2022, 10, 25, 15, 35, 30)},
            {'start': datetime(2002, 5, 24, 1, 34, 20), 'end': datetime(2002, 5, 31, 1, 20, 20)},
            {'start':  datetime(2002, 8, 16, 16, 16, 16), 'end': datetime(2003, 1, 1, 1, 1, 1)},
        ],
        expected=[
            {'start': datetime(2022, 10, 20, 15, 35, 30), 'end': datetime(2022, 10, 25, 15, 35, 30), 'hours': 120},
            {
                'start': datetime(2002, 5, 24, 1, 34, 20),
                'end': datetime(2002, 5, 31, 1, 20, 20),
                'hours': approx(167.7666666, 0.001)
            },
            {
                'start':  datetime(2002, 8, 16, 16, 16, 16),
                'end': datetime(2003, 1, 1, 1, 1, 1),
                'hours': approx(3296.745833333, 0.001)
            },
        ],
        cmp_keys=('column', 'datetime')
    ),
]


@pytest.mark.parametrize('case', MAP_CASES)
def test_mapper(case: MapCase) -> None:
    mapper_data_row = copy.deepcopy(case.data[case.mapper_item])
    mapper_expected_rows = [copy.deepcopy(case.expected[i]) for i in case.mapper_expected_items]
    key_func = _Key(*case.cmp_keys)

    mapper_result = case.mapper(mapper_data_row)

    assert isinstance(mapper_result, tp.Iterator)
    assert sorted(mapper_expected_rows, key=key_func) == sorted(mapper_result, key=key_func)

    result = ops.Map(case.mapper)(iter(case.data))

    assert isinstance(result, tp.Iterator)
    assert sorted(case.expected, key=key_func) == sorted(result, key=key_func)
