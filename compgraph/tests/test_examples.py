import os

from click.testing import CliRunner

from examples.run_word_count import word_count
from examples.run_inverted_index import inverted_index
from examples.run_pmi import pmi
from examples.run_yandex_maps import yandex_maps


def test_word_count() -> None:
    runner = CliRunner()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..')

    input_ = os.path.join(project_root, 'tests', 'data_for_examples', 'canondata', 'text_corpus.jsonl')

    result = runner.invoke(word_count, f'--input {input_}')
    assert result.exit_code == 0

    answer = os.path.join(project_root, 'tests', 'data_for_examples', 'answers', 'word_count.jsonl')

    test_output_ = result.output.rstrip()

    with open(answer, 'r') as canon_res:
        canon_output_ = canon_res.read()

    assert test_output_ == canon_output_


def test_invert_index() -> None:
    runner = CliRunner()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..')

    input_ = os.path.join(project_root, 'tests', 'data_for_examples', 'canondata', 'text_corpus.jsonl')

    result = runner.invoke(inverted_index, f'--input {input_}')
    assert result.exit_code == 0

    answer = os.path.join(project_root, 'tests', 'data_for_examples', 'answers', 'inverted_index.jsonl')

    test_output_ = result.output.rstrip()

    with open(answer, 'r') as canon_res:
        canon_output_ = canon_res.read()

    assert test_output_ == canon_output_


def test_pmi() -> None:
    runner = CliRunner()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..')

    input_ = os.path.join(project_root, 'tests', 'data_for_examples', 'canondata', 'text_corpus.jsonl')

    result = runner.invoke(pmi, f'--input {input_}')
    assert result.exit_code == 0

    answer = os.path.join(project_root, 'tests', 'data_for_examples', 'answers', 'pmi.jsonl')
    test_output_ = result.output.rstrip()

    with open(answer, 'r') as canon_res:
        canon_output_ = canon_res.read()

    assert test_output_ == canon_output_


def test_yandex_maps() -> None:
    runner = CliRunner()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(script_dir, '..')

    input_time = os.path.join(project_root, 'tests', 'data_for_examples', 'canondata', 'travel_times.jsonl')
    input_length = os.path.join(project_root, 'tests', 'data_for_examples', 'canondata', 'road_graph_data.jsonl')

    result = runner.invoke(yandex_maps, f'--input_time {input_time} --input_length {input_length}')
    assert result.exit_code == 0

    answer = os.path.join(project_root, 'tests', 'data_for_examples', 'answers', 'yandex_maps.jsonl')
    test_output_ = result.output.rstrip()

    with open(answer, 'r') as canon_res:
        canon_output_ = canon_res.read()

    assert test_output_ == canon_output_
