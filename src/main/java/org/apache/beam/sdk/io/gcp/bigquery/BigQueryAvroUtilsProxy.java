package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;

public class BigQueryAvroUtilsProxy {

    public static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
        return BigQueryAvroUtils.convertGenericRecordToTableRow(record, schema);
    }
}
