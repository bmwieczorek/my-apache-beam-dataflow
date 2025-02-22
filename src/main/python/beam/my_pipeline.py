import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# based on
# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount.py

# pip install pytest apache-beam['gcp']

# direct runner
# python3 src/main/python/beam/my_pipeline.py

# dataflow runner
# python3 src/main/python/beam/my_pipeline.py --project ${GCP_PROJECT} \
# --region ${GCP_REGION} --runner DataflowRunner \
# --temp_location gs://${GCP_PROJECT}-${GCP_OWNER}/python-tmp/ \
# --service_account_email=${GCP_SERVICE_ACCOUNT} --no_use_public_ips \
# --subnetwork ${GCP_SUBNETWORK}



def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:
        # noinspection PyUnresolvedReferences
        words = p | 'Create' >> beam.Create(["hi", "there", "bob"])
        # noinspection PyUnresolvedReferences
        words | 'Log' >> beam.Map(lambda word: logging.info(word))


if __name__ == '__main__':
    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s')
    run()
