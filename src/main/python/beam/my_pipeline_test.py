import unittest
import apache_beam as beam
from apache_beam import FlatMap, CoGroupByKey

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


# based on https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
# pip install pytest apache-beam[gcp]
# python3 src/main/python/my_pipeline_test.py
# python3 -m pytest src/main/python/my_pipeline_test.py -o log_cli=true -v --junit-xml=target/TEST-results.xml

def inner_join(element):
    key = element[0]
    _dict = element[1]
    _m_list = _dict['m']
    _s_list = _dict['s']
    print(key, _m_list, _s_list)
    return [{"key": key, "m": _m, "s": _s} for _m in _m_list for _s in _s_list]


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

    def test_join(self):
        with TestPipeline() as p:
            m = p | "M" >> beam.Create([('a', 1), ('b', 2), ('c', 3)])
            s = p | "S" >> beam.Create([('b', 11), ('c', 22), ('c', 33), ('d', 44)])
            cogbk = {"m": m, "s": s} | CoGroupByKey()
            output = cogbk | FlatMap(inner_join)
            assert_that(output, equal_to([
                {'key': 'b', 'm': 2, 's': 11},
                {'key': 'c', 'm': 3, 's': 22},
                {'key': 'c', 'm': 3, 's': 33}
            ]))





if __name__ == '__main__':
    unittest.main()
