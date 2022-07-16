package com.bawi.beam.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

// https://github.com/rishisinghal/BeamPipelineSamples/blob/master/src/main/java/com/sample/beam/df/utils/TableRowWithSchemaCoder.java
public class TableRowWithSchemaCoder extends CustomCoder<TableRowWithSchema> {

	private static final long serialVersionUID = 1L;

	// Coders for the member types
	private static final StringUtf8Coder TABLE_NAME_CODER = StringUtf8Coder.of();
	private static final GenericJsonCoder<TableSchema> TABLE_SCHEMA_CODER = GenericJsonCoder.of(TableSchema.class);
	private static final GenericJsonCoder<TableRow> TABLE_ROW_CODER = GenericJsonCoder.of(TableRow.class);

	// Singleton instances
	private static final TableRowWithSchemaCoder INSTANCE = new TableRowWithSchemaCoder();
	private static final TypeDescriptor<TableRowWithSchema> TYPE_DESCRIPTOR = new TypeDescriptor<>() {
		private static final long serialVersionUID = 1L;
	};

	public static TableRowWithSchemaCoder of() {
		return INSTANCE;
	}

	private TableRowWithSchemaCoder() {
	}

	@Override
	public void encode(TableRowWithSchema value, OutputStream outStream) throws IOException {
		if (value == null) {
			throw new CoderException("The TableRowWithSchemaCoder cannot encode a null object!");
		}

		TABLE_NAME_CODER.encode(value.getDataType(), outStream);
		TABLE_SCHEMA_CODER.encode(value.getTableSchema(), outStream);
		TABLE_ROW_CODER.encode(value.getTableRow(), outStream);
	}

	@Override
	public TableRowWithSchema decode(InputStream inStream) throws IOException {
		String dataType = TABLE_NAME_CODER.decode(inStream);
		TableSchema tableSchema = TABLE_SCHEMA_CODER.decode(inStream);
		TableRow tableRow = TABLE_ROW_CODER.decode(inStream);

		return TableRowWithSchema.newBuilder().setDataType(dataType).setTableSchema(tableSchema).setTableRow(tableRow)
				.build();
	}

	@Override
	public TypeDescriptor<TableRowWithSchema> getEncodedTypeDescriptor() {
		return TYPE_DESCRIPTOR;
	}

}
