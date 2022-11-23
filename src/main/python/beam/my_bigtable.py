import argparse
import os

import apache_beam as beam
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud.bigtable import row
import datetime


# pip install pytest apache-beam['gcp']

# direct runner
# python3 src/main/python/beam/my_pipeline.py

# dataflow runner
# python3 src/main/python/beam/my_pipeline.py --project ${GCP_PROJECT} \
# --region ${GCP_REGION} --runner DataflowRunner \
# --temp_location gs://${GCP_PROJECT}-${GCP_OWNER}/python-tmp/ \
# --service_account_email=${GCP_SERVICE_ACCOUNT} --no_use_public_ips


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        words = p | beam.Create([create_direct_row()])
        words | WriteToBigTable(project_id=os.getenv('GCP_PROJECT'),
                                instance_id='my-instance',
                                table_id='my-table')


def create_direct_row():
    direct_row = row.DirectRow(row_key='myKey5'.encode('UTF-8'))
    direct_row.set_cell(
        'my-column-family1',
        'my-column-qualifier2'.encode('UTF-8'),
        'aaa111'.encode('UTF-8'),
        timestamp=datetime.datetime.now())
    return direct_row


if __name__ == '__main__':
    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s')
    run()
