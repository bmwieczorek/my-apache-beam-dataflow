import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# based on https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py
# pip install pytest apache-beam['gcp']
# python3 src/main/python/beam/my_pipeline.py
# python3 src/main/python/beam/my_pipeline.py --project ${GCP_PROJECT} --region ${GCP_REGION} --runner DataflowRunner \
# --temp_location gs://${GCP_PROJECT}-${GCP_OWNER}/python-tmp/ --service_account_email=${GCP_SERVICE_ACCOUNT} \
# --no_use_public_ips

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        # IDE unnecessary warning as PTransform implements __ror__
        # noinspection PyUnresolvedReferences
        words = p | 'Create' >> beam.Create(["hi", "there", "bob"])
        words | 'Log' >> beam.Map(lambda word: logging.info(word))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
