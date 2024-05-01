import json

from . import Graph
from . import operations


def read_graph(input_stream_name: str, from_file: bool) -> Graph:
    if from_file:
        graph = Graph.graph_from_file(input_stream_name, json.loads)
    else:
        graph = Graph.graph_from_iter(input_stream_name)

    return graph


def PreparedGraph(
    input_stream_name: str,
    text_column: str,
    from_file: bool
) -> Graph:
    graph = read_graph(input_stream_name, from_file)

    return graph \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))


def word_count_graph(
    input_stream_name: str, text_column: str = 'text',
    count_column: str = 'count', from_file: bool = False
) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return PreparedGraph(input_stream_name, text_column, from_file) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(
    input_stream_name: str, doc_column: str = 'doc_id',
    text_column: str = 'text', result_column: str = 'tf_idf',
    from_file: bool = False
) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""

    doc_graph = read_graph(input_stream_name, from_file) \
        .reduce(operations.Count('docs_count'), [])

    words_graph = PreparedGraph(input_stream_name, text_column, from_file)

    idf = words_graph.sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count('docs_for_word'), [text_column]) \
        .join(operations.InnerJoiner(), doc_graph, []) \
        .map(operations.FractionLog(['docs_count', 'docs_for_word'], 'idf'))

    return words_graph.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column]) \
        .join(operations.InnerJoiner(), idf, [text_column]) \
        .map(operations.Product(['tf', 'idf'], result_column)) \
        .reduce(operations.TopN(result_column, 3), [text_column]) \
        .map(operations.Project([doc_column, text_column, result_column])) \



def pmi_graph(
    input_stream_name: str, doc_column: str = 'doc_id',
    text_column: str = 'text', result_column: str = 'pmi', top_words: int = 10,
    min_len: int = 4, min_occur: int = 2, from_file: bool = False
) -> Graph:

    """
    Constructs graph which gives for every document the top 10 words
    ranked by pointwise mutual information
    """

    words_graph = PreparedGraph(input_stream_name, text_column, from_file) \
        .map(operations.Filter(lambda r: len(r[text_column]) > min_len)) \
        .sort([doc_column, text_column]) \
        .reduce(operations.Count('words_doc'), [doc_column, text_column]) \
        .map(operations.Filter(lambda r: r['words_doc'] >= min_occur))

    word_i = words_graph.sort([text_column]) \
        .reduce(operations.Sum('words_doc'), [text_column]) \
        .map(operations.CopyWithDelete('words_doc', 'word_i'))

    doc_j = words_graph.sort([doc_column]) \
        .reduce(operations.Sum('words_doc'), [doc_column]) \
        .map(operations.CopyWithDelete('words_doc', 'doc_j'))

    total = words_graph.reduce(operations.Sum('words_doc'), []) \
        .map(operations.CopyWithDelete('words_doc', 'total'))

    log = words_graph.sort([text_column]) \
        .join(operations.InnerJoiner(), word_i, [text_column]) \
        .join(operations.InnerJoiner(), total, []) \
        .sort([doc_column]) \
        .join(operations.InnerJoiner(), doc_j, [doc_column]) \
        .map(operations.Product(['words_doc', 'total'], 'total_words_doc')) \
        .map(operations.Product(['doc_j', 'word_i'], 'doc_j_word_i')) \
        .map(operations.FractionLog(
            ['total_words_doc', 'doc_j_word_i'],
            result_column
        ))

    return log.sort([doc_column, result_column]) \
        .reduce(operations.TopN(result_column, top_words), [doc_column]) \
        .map(operations.Project([doc_column, text_column, result_column]))


def yandex_maps_graph(
    input_stream_name_time: str, input_stream_name_length: str,
    enter_time_column: str = 'enter_time',
    leave_time_column: str = 'leave_time',
    edge_id_column: str = 'edge_id', start_coord_column: str = 'start',
    end_coord_column: str = 'end',
    weekday_result_column: str = 'weekday',
    hour_result_column: str = 'hour',
    speed_result_column: str = 'speed',
    time_format: str = '%Y%m%dT%H%M%S.%f', from_file: bool = False
) -> Graph:
    """
    Constructs graph which measures average speed in km/h
    depending on the weekday and hour
    """

    length = read_graph(input_stream_name_length, from_file) \
        .map(operations.Haversine(
            [start_coord_column, end_coord_column],
            'length'
        )) \
        .sort([edge_id_column])

    time = read_graph(input_stream_name_time, from_file) \
        .map(operations.Strptime(
            [enter_time_column], time_format, 'enter_date'
        )).map(operations.Strptime(
            [leave_time_column], time_format, 'leave_date'
        )).map(operations.Project(
            ['enter_date', 'leave_date', edge_id_column]
        )).map(operations.Strftime(
            ['enter_date'], '%a', weekday_result_column
        )).map(operations.Strftime(
            ['enter_date'], '%H', hour_result_column
        )).sort([edge_id_column]) \
        .map(operations.CalcHours(
            ['enter_date', 'leave_date'], 'time'
        )).map(operations.Project(
            [weekday_result_column, hour_result_column, 'time']
        )).sort([weekday_result_column, hour_result_column]) \
        .reduce(
            operations.Sum('time'),
            [weekday_result_column, hour_result_column]
        )

    distance = read_graph(input_stream_name_time, from_file) \
        .map(operations.Strptime(
            [enter_time_column], time_format, 'enter_date'
        )).map(operations.Strftime(
            ['enter_date'], '%a', weekday_result_column
        )).map(operations.Strftime(
            ['enter_date'], '%H', hour_result_column
        )).map(operations.Project(
            [weekday_result_column, hour_result_column, edge_id_column]
        )).sort([edge_id_column]) \
        .join(operations.InnerJoiner(), length, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(
            operations.Sum('length'),
            [weekday_result_column, hour_result_column]
        )

    return distance.join(
        operations.InnerJoiner(),
        time, [weekday_result_column, hour_result_column]
    ).map(operations.Divide(
        ['length', 'time'], speed_result_column
    )).map(operations.Project(
        [weekday_result_column, hour_result_column, speed_result_column]
    ))
