import unittest
from typing import Tuple, List, Dict, Union

import apache_beam as beam
from apache_beam import FlatMap, CoGroupByKey, pvalue, PCollection, Map

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


# based on https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
# pip install pytest apache-beam[gcp]
# python3 src/main/python/beam/my_pipeline_test.py
# python3 -m pytest src/main/python/beam/my_pipeline_test.py -o log_cli=true -v --junit-xml=target/TEST-results.xml


def inner_join(element: Tuple[str, Dict[str, List[int]]]) -> List[Dict[str, Union[str, int]]]:
    key = element[0]
    _dict = element[1]
    _m_list = _dict['m']
    _s_list = _dict['s']
    print(element, key, _m_list, _s_list)
    return [{"key": key, "m": _m, "s": _s} for _m in _m_list for _s in _s_list]


def broadcast_join(element: Tuple[str, int], side_input: Dict[str, int]) -> Tuple[str, int]:
    key = element[0]
    val = element[1]
    _multiplier = side_input.get(key, 1)
    print(key, val, side_input, _multiplier)
    return key, val * _multiplier


# noinspection PyUnresolvedReferences
class MyPipelineTransformationTest(unittest.TestCase):

    # noinspection PyMethodMayBeStatic
    def test_count_per_element(self):
        # Create a test pipeline.
        with TestPipeline() as p:
            words = p | beam.Create(['a', 'b', 'a'])
            output = words | beam.combiners.Count.PerElement()
            assert_that(output, equal_to([('a', 2), ('b', 1)]))
            # The pipeline will run and verify the results.

    def test_count_per_key(self):
        with TestPipeline() as p:
            words = p | beam.Create([('a', 1), ('b', 1), ('a', 1)])
            output = words | beam.combiners.Count.PerKey()
            assert_that(output, equal_to([('a', 2), ('b', 1)]))

    def test_sum_per_key(self):
        with TestPipeline() as p:
            words = p | beam.Create([('a', 1), ('b', 1), ('a', 2)])
            output = words | beam.CombinePerKey(sum)
            assert_that(output, equal_to([('a', 3), ('b', 1)]))

    def test_group_per_key_and_sum(self):
        with TestPipeline() as p:
            words = p | beam.Create([('a', 1), ('b', 1), ('a', 2)])
            output = words | beam.GroupByKey() | beam.MapTuple(lambda k, vs: (k, sum(vs)))
            assert_that(output, equal_to([('a', 3), ('b', 1)]))

    def test_group_per_key_and_count(self):
        with TestPipeline() as p:
            words = p | beam.Create([('a', 1), ('b', 1), ('a', 2)])
            output = words | beam.GroupByKey() | beam.MapTuple(lambda k, vs: (k, len(vs)))
            assert_that(output, equal_to([('a', 2), ('b', 1)]))

    def test_group_per_dictionary_key(self):
        with TestPipeline() as p:
            words = p | beam.Create([{'k1': 'a', 'v': 'a1'}, {'k1': 'b', 'v': 'b2'}, {'k1': 'a', 'v': 'a11'}])
            output = words | beam.GroupBy(lambda d: d['k1'])
            assert_that(output, equal_to([
                ('a', [{'k1': 'a', 'v': 'a1'}, {'k1': 'a', 'v': 'a11'}]),
                ('b', [{'k1': 'b', 'v': 'b2'}])
            ]))

    def test_join_using_cogroupbykey(self):
        with TestPipeline() as p:
            m: PCollection[Tuple[str, int]] = p | "M" >> beam.Create([('a', 1), ('b', 2), ('c', 3), ('c', 4)])
            s: PCollection[Tuple[str, int]] = p | "S" >> beam.Create([('b', 11), ('c', 22), ('d', 33)])
            cogbk: PCollection[Tuple[str, Dict[str, List[int]]]] = {"m": m, "s": s} | CoGroupByKey()
            output: PCollection[Dict[str, Union[str, int]]] = cogbk | FlatMap(inner_join)
            assert_that(output, equal_to([
                {'key': 'b', 'm': 2, 's': 11},
                {'key': 'c', 'm': 3, 's': 22},
                {'key': 'c', 'm': 4, 's': 22}
            ]))

    def test_join_using_sideinput(self):
        with TestPipeline() as p:
            m = p | "M" >> beam.Create([('a', 1), ('b', 2), ('c', 3), ('c', 4)])
            s = p | "S" >> beam.Create([('b', 11), ('c', 22), ('d', 33)])
            joined: PCollection[Tuple[str, int]] = m | Map(broadcast_join, side_input=pvalue.AsDict(s))
            assert_that(joined, equal_to([('a', 1), ('b', 22), ('c', 66), ('c', 88)]))


if __name__ == '__main__':
    unittest.main()
