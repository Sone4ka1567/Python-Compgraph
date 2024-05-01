import click
import sys

from compgraph.algorithms import yandex_maps_graph


@click.command()
@click.option("--input_time", type=str, required=True)
@click.option("--input_length", type=str, required=True)
@click.option("--output", type=click.File("w"), required=False)
def yandex_maps(
    input_time: str,
    input_length: str,
    output: str
) -> None:
    graph = yandex_maps_graph(
        input_stream_name_time=input_time,
        input_stream_name_length=input_length,
        from_file=True
    )

    result = graph.run()

    output = output or sys.stdout  # type: ignore

    for row in result:
        print(row, file=output)  # type: ignore


if __name__ == "__main__":
    yandex_maps()
