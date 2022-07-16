package com.bawi.beam.dataflow.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

// https://github.com/rishisinghal/BeamPipelineSamples/blob/master/src/main/java/com/sample/beam/df/utils/TableRowWithSchema.java
public final class TableRowWithSchema {

	private final String dataType;
	private final TableSchema tableSchema;
	private final TableRow tableRow;

	private TableRowWithSchema(String dataType, TableSchema tableSchema, TableRow tableRow) {
		this.dataType = dataType;
		this.tableSchema = tableSchema;
		this.tableRow = tableRow;
	}

	public String getDataType() {
		return dataType;
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public TableRow getTableRow() {
		return tableRow;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	@Override
	public String toString() {
		return "TableRowWithSchema{dataType=" + dataType + ", " + "tableSchema=" + tableSchema + ", " + "tableRow="
				+ tableRow + "}";
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o instanceof TableRowWithSchema) {
			TableRowWithSchema that = (TableRowWithSchema) o;
			return (this.dataType.equals(that.getDataType())) && (this.tableSchema.equals(that.getTableSchema()))
					&& (this.tableRow.equals(that.getTableRow()));
		}
		return false;
	}

	@Override
	public int hashCode() {
		int h = 1;
		h *= 1000003;
		h ^= this.dataType.hashCode();
		h *= 1000003;
		h ^= this.tableSchema.hashCode();
		h *= 1000003;
		h ^= this.tableRow.hashCode();
		return h;
	}

	public static final class Builder {
		private String dataType;
		private TableSchema tableSchema;
		private TableRow tableRow;

		public TableRowWithSchema.Builder setDataType(String dataType) {
			this.dataType = dataType;
			return this;
		}

		public TableRowWithSchema.Builder setTableSchema(TableSchema tableSchema) {
			this.tableSchema = tableSchema;
			return this;
		}

		public TableRowWithSchema.Builder setTableRow(TableRow tableRow) {
			this.tableRow = tableRow;
			return this;
		}

		public TableRowWithSchema build() {
			String missing = "";
			if (this.dataType == null) {
				missing += " dataType";
			}
			if (this.tableSchema == null) {
				missing += " tableSchema";
			}
			if (this.tableRow == null) {
				missing += " tableRow";
			}
			if (!missing.isEmpty()) {
				throw new IllegalStateException("Missing required properties:" + missing);
			}
			return new TableRowWithSchema(this.dataType, this.tableSchema, this.tableRow);
		}
	}
}
