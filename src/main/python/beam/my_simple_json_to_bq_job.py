import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import FileFormat, parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions


class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--output_table', type=str,
                                           help='Output table in format project.dataset.table')


class MyDoFn(beam.DoFn):
    def process(self, element, **kwargs):
        parsed_data = json.loads(element)
        logging.info("INFO loading JSON data from %s to %s", element, type(parsed_data))
        yield parsed_data


def run():
    """Main entry point"""
    pipeline_options = PipelineOptions()
    my_pipeline_options = pipeline_options.view_as(MyPipelineOptions)

    # json type needs to be lower case
    bq_json_schema_string = """{
        "fields": [
            {"name": "user_id", "type": "STRING"},
            {"name": "event_properties", "type": "json"}
        ]
    }"""
    bq_table_schema = parse_table_schema_from_json(bq_json_schema_string)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=my_pipeline_options) as p:
        # lines = p | 'ReadJSON' >> beam.io.ReadFromText("sample_input__00.json")
        # noinspection PyUnresolvedReferences
        lines = p | 'CreateJsonStrings' >> beam.Create([
            '{"user_id": "id1", "event_properties": {"myStrProp1": "myVal1", "myIntProp": 1000}}',
            '{"user_id": "id2", "event_properties": {"myStrProp2": "myVal2", "myIntProp": 2000}}'
        ])
        # noinspection PyUnresolvedReferences
        # processed = lines | 'LoadToJson' >> beam.Map(lambda line: json.loads(line))
        # noinspection PyTypeChecker
        processed = lines | 'LoadToJson' >> beam.ParDo(MyDoFn())
        processed | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=my_pipeline_options.output_table,
            schema=bq_table_schema,
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            temp_file_format=FileFormat.JSON
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# direct runner
# python3 src/main/python/beam/my_simple_json_to_bq_job.py \
#  --temp_location "gs://${GCP_PROJECT}-${GCP_OWNER}/python-tmp" \
#  --output_table "${GCP_PROJECT}.bartek_person.my_json_table4"

# dataflow runner
# python3 src/main/python/beam/my_simple_json_to_bq_job.py \
#  --runner DataflowRunner \
#  --job_name my-simple-json-to-bq-python-job \
#  --project ${GCP_PROJECT} \
#  --region ${GCP_REGION} \
#  --service_account_email=${GCP_SERVICE_ACCOUNT} \
#  --no_use_public_ips \
#  --subnetwork ${GCP_SUBNETWORK} \
#  --temp_location gs://${GCP_PROJECT}-${GCP_OWNER}/python-tmp \
#  --output_table ${GCP_PROJECT}.bartek_person.my_json_table4
