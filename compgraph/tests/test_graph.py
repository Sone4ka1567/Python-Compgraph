from compgraph import operations as ops
from compgraph.graph import Graph


def test_basic_map() -> None:
    graph = Graph.graph_from_iter('docs').map(ops.DummyMapper())

    docs = [
        {'doc_id': 1, 'text': '1'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 4, 'text': '4'},
    ]

    expected = [
        {'doc_id': 1, 'text': '1'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 4, 'text': '4'},
    ]

    result = graph.run(docs=lambda: iter(docs))
    assert expected == list(result)


def test_basic_reduce() -> None:
    graph = Graph.graph_from_iter('docs').reduce(ops.FirstReducer(), ['doc_id'])

    docs = [
        {'doc_id': 1, 'text': '1'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 4, 'text': '4'},
    ]

    expected = [
        {'doc_id': 1, 'text': '1'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 4, 'text': '4'},
    ]

    result = graph.run(docs=lambda: iter(docs))
    assert expected == list(result)


def test_sort() -> None:
    graph = Graph.graph_from_iter('docs').sort(['doc_id'])

    docs = [
        {'doc_id': 4, 'text': '4'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 1, 'text': '1'}
    ]

    expected = [
        {'doc_id': 1, 'text': '1'},
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 3, 'text': '3'},
        {'doc_id': 4, 'text': '4'},
    ]

    result = graph.run(docs=lambda: iter(docs))
    assert expected == list(result)


def test_inner_join() -> None:
    graph1 = Graph.graph_from_iter('docs')
    graph2 = Graph.graph_from_iter('docs').join(ops.InnerJoiner(), graph1, keys=[])

    docs = [
        {'doc_id': 2, 'text': '2'},
        {'doc_id': 1, 'text': '1'}
    ]

    expected = [
        {'doc_id_1': 2, 'doc_id_2': 2,
         'text_1': '2', 'text_2': '2'},
        {'doc_id_1': 2, 'doc_id_2': 1,
         'text_1': '2', 'text_2': '1'},
        {'doc_id_1': 1, 'doc_id_2': 2,
         'text_1': '1', 'text_2': '2'},
        {'doc_id_1': 1, 'doc_id_2': 1,
         'text_1': '1', 'text_2': '1'}
    ]

    result = graph2.run(docs=lambda: iter(docs))
    assert expected == list(result)


def test_multiple_start() -> None:
    graph = Graph.graph_from_iter('docs').map(ops.LowerCase('text'))

    docs = [
        {'doc_id': 1, 'text': 'hello WORLD!!!'},
        {'doc_id': 2, 'text': '?Hello hEllO?'}
    ]

    expected1 = [
        {'doc_id': 1, 'text': 'hello world!!!'},
        {'doc_id': 2, 'text': '?hello hello?'}
    ]

    result1 = graph.run(docs=lambda: iter(docs))

    assert expected1 == list(result1)

    expected2 = [
        {'doc_id': 1, 'text': 'hello world'},
        {'doc_id': 2, 'text': 'hello hello'},
    ]

    result2 = graph.map(ops.FilterPunctuation('text')).run(docs=lambda: iter(expected2))

    assert expected2 == list(result2)
