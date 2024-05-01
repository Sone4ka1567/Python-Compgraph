import click
import sys

from compgraph.algorithms import inverted_index_graph


@click.command()
@click.option("--input", type=str, required=True)
@click.option("--output", type=click.File("w"), required=False)
def inverted_index(input: str, output: str) -> None:
    graph = inverted_index_graph(input_stream_name=input, from_file=True)

    result = graph.run()

    output = output or sys.stdout  # type: ignore

    for row in result:
        print(row, file=output)  # type: ignore


if __name__ == "__main__":
    inverted_index()
