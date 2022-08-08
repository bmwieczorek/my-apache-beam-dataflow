import unittest
import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# based on https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
# pip install pytest apache-beam['gcp']


class MyPipelineTransformationTest(unittest.TestCase):
    def test_count(self):
        # Create a test pipeline.
        with TestPipeline() as p:
            input = p | beam.Create(["a", "b", "a"])
            output = input | beam.combiners.Count.PerElement()
            assert_that(output, equal_to([("a", 2), ("b", 1)]))
            # The pipeline will run and verify the results.


if __name__ == '__main__':
    unittest.main()
