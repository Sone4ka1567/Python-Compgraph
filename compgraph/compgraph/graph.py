import typing as tp

from . import operations as ops
from compgraph.external_sort import ExternalSort


class Graph:
    """Computational graph implementation"""

    def __init__(self) -> None:
        self.operation: ops.Operation | None = None
        self.main_graph: Graph | None = None
        self.another_graph: Graph | None = None

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator
        (in form of sequence of Rows from 'kwargs' passed to 'run' method)
        into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph = Graph()
        graph.operation = ops.ReadIterFactory(name)
        return graph

    @staticmethod
    def graph_from_file(
        filename: str, parser: tp.Callable[[str], ops.TRow]
    ) -> 'Graph':
        """Construct new graph extended with operation
        for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph = Graph()
        graph.operation = ops.Read(filename, parser)
        return graph

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation
        with particular mapper
        :param mapper: mapper to use
        """
        graph = Graph()
        graph.operation = ops.Map(mapper)
        graph.main_graph = self

        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce
        operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph = Graph()
        graph.operation = ops.Reduce(reducer, keys)
        graph.main_graph = self

        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph = Graph()
        graph.operation = ExternalSort(keys)
        graph.main_graph = self

        return graph

    def join(
        self, joiner: ops.Joiner,
        join_graph: 'Graph', keys: tp.Sequence[str]
    ) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph = Graph()
        graph.operation = ops.Join(joiner, keys)
        graph.main_graph = self
        graph.another_graph = join_graph

        return graph

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        if self.main_graph is None:
            return self.operation(**kwargs)  # type: ignore

        if self.another_graph is None:
            return self.operation(self.main_graph.run(**kwargs))  # type: ignore

        return self.operation(  # type: ignore
            self.main_graph.run(**kwargs),
            self.another_graph.run(**kwargs)
        )
