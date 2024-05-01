## Compgraph library

### Description

A simple library with Map / Reduce operation implementations, and computational graph interface. Also the library contains several algorithms for popular problems, such as tf-idf, word counting and etc.

A table is a sequence of dictionaries, where each dictionary represents a row in the table, and the dictionary key represents the table column
(the index in the sequence + key in the dictionary define a cell).

We will execute calculations on tables by applying *computational graphs*.

By computational graph, we mean a predetermined sequence of operations, which can then be applied to various datasets.

Why do we need computational graphs anyway?

Computational graphs allow separating the description of the sequence of operations from their execution. Thanks to this, you can both
run operations in a different environment (for example, describe the graph in a Python interpreter, and then perform it on a GPU),
and independently and concurrently run on multiple machines of a computational cluster to process a large array
of input data in a reasonable finite time (for example, this is how the Spark distributed computing system client works).

Pay attention to the graph interface; each operation is defined by calling the corresponding method of the Graph class
(see the full list of operations in [compgraph/graph.py](compgraph/graph.py)).


### How to install

run ```pip install -e compgraph --force-reinstall``` in 09.2.HW2/tasks/ or run ```pip install -e . --force-reinstall ``` in 09.2.HW2/tasks/compgraph

### Algorithms description

- word_count_graph

You are given a table with rows in the format {'doc_id': ..., 'text': ...} or other format.
You need to count the total occurrences of each word found in the 'text' column throughout the entire table.

- inverted_index_graph

For this collection, we will build an *inverted index* - a data structure that stores a sorted list of documents for each word ordered by *relevance*.

We will measure relevance using the [tf-idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) metric.

For each pair (word, document), we will define tf-idf as follows:

TFIDF(word_i, doc_j) = (frequency of word_i in doc_j) * ln((total number of docs) / (docs where word_i is present))

For each word, we need to calculate the top-3 documents by tf-idf.


- pmi_graph

Op words with the highest mutual information

This task is the opposite of the previous one: for each document, calculate the top-10 most characteristic words for it.

We will rank words by the [Pointwise mutual information](https://en.wikipedia.org/wiki/Pointwise_mutual_information) metric.

More formally, the task is as follows: for each text doc_j, find the top-10 words word_i that satisfy both conditions:
1) word_i is strictly longer than four characters;
2) word_i occurs in the document doc_j at least twice.
Top words word_i should be selected based on the value:

pmi(word_i, doc_j) = ln((frequency of word_i in doc_j) / (frequency of word_i in all documents combined))

The higher this value, the more characteristic word_i is considered for doc_j.


- yandex_maps_graph

Average traffic speed in the city depending on the hour and day of the week

In this task, you have to work with information about the movement of people in cars on a certain subset of the streets of Moscow.

The city streets are represented as a graph, and the movement information is given as a table, with each row containing data like

{'edge_id': '624', 'enter_time': '20170912T123410.1794', 'leave_time': '20170912T123412.68'}

where edge_id is the identifier of the road graph edge (i.e., just a section of a street), and enter_time and leave_time are
the time of entering and leaving the edge, respectively (time in UTC).

You are also given an auxiliary table of the form

{'edge_id': '313', 'length':121, 'start': [37.31245, 51.256734], 'end': [37.31245, 51.256734]}

where length is in meters, and start and end are the coordinates of the beginning and end points of the edge, given in the format ('lon', 'lat').
Perhaps not all the edges of the graph have all the meta-information, so you should look for the distance yourself.

Note: The distance between points is suggested to be found using the [haversine distance](https://en.wikipedia.org/wiki/Haversine_formula), and the Earth's radius should be considered as 6373 km (compare with the tests).

Using this information, you need to build a table with the average traffic speed in km/h in the city, depending on the hour and day of the week; you need to extract these two parameters from enter_time.


### Usage examples

We have a [directory](resources) with some example data for all 4 implemented algorithms. Also we have ready-to-go scripts for their usage in [examples](examples). The results are in [examples/results](examples/results)

- for algorithms word_count_graph, inverted_index_graph and pmi_graph run ```python ./examples/run_word_count.py --input resources/text_corpus.txt --output examples/results/word_count.txt ```

- for yandex_maps_graph algorithm run ```python ./examples/run_yandex_maps.py --input_time resources/travel_times.txt --input_length resources/road_graph_data.txt --output examples/results/yandex_maps.txt```

### Tests

We achived more than [95% test coverage](tests/coverage.png). If you want to run unit_tests run ```pytest compgraph``` from 09.2.HW2/tasks/ or ```pytest compgraph --cov=compgraph --cov-fail-under=95 ``` if ypu want to see coverage percentage.