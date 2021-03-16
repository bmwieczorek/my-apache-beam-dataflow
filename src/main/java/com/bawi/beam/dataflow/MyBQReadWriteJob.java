package com.bawi.beam.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static com.bawi.beam.dataflow.MyBQReadWriteJob.MySubscription.SCHEMA;

public class MyBQReadWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBQReadWriteJob.class);

    public interface MyBQOptions extends PipelineOptions {
        @Validation.Required
        ValueProvider<String> getExpirationDate();
        void setExpirationDate(ValueProvider<String> value);

        @Validation.Required
        @Default.String("bartek_dataset.mysubscription_table")
        String getTable();
        void setTable(String value);
    }
    
    public static void main(String[] args) {
//        System.setProperty("java.util.logging.config.file", "src/main/resources/logging.properties");

        MyBQOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyBQOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        String table = pipelineOptions.getTable();
        pipeline.apply(BigQueryIO.read(schemaAndRecord -> {
                                GenericRecord genericRecord = schemaAndRecord.getRecord();
                                // avro schema from BQ converts timestamp to micro seconds
                                MySubscription mySubscription = MySubscription.fromGenericRecord(genericRecord);
                                LOGGER.info("Read {}, with schema {}", mySubscription, schemaAndRecord.getTableSchema());
                                return mySubscription;
                            }
                        )
                        .fromQuery(ValueProvider.NestedValueProvider.of(pipelineOptions.getExpirationDate(), expirationDate -> getQuery(table, expirationDate)))
                        // non default settings below:
                        .useAvroLogicalTypes() // convert BQ TIMESTAMP to avro long millis/micros and BQ DATE to avro int
                        .withoutValidation() // skip validation if using value provider for query
                        .usingStandardSql() // required for TIMESTAMP function - needs to be below .fromQuery
                        .withTemplateCompatibility() // required to re-run jobs from templates
                        //.from(pipelineOptions.getTableName()))  // all data
                    )

                .apply("To GenericRecords", MapElements.into(TypeDescriptor.of(GenericRecord.class)).via(MySubscription::toGenericRecord))
                .setCoder(AvroGenericCoder.of(MySubscription.SCHEMA))

                // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
                .apply(BigQueryIO.<GenericRecord>write()
                        .withAvroFormatFunction(r -> {
                            GenericRecord element = r.getElement();
                            LOGGER.info("element {}, schema {}", element, r.getSchema());
                            return element;
                        })
                        .withAvroSchemaFactory(qTableSchema -> SCHEMA)
                        .to(table)
                        .useAvroLogicalTypes()
                        .withSchema(new TableSchema().setFields(
                                Arrays.asList(
                                    new TableFieldSchema().setName("id").setType("STRING").setMode("REQUIRED"),
                                    new TableFieldSchema().setName("creation_timestamp").setType("TIMESTAMP").setMode("REQUIRED"),
                                    new TableFieldSchema().setName("expiration_date").setType("DATE").setMode("REQUIRED")
                                )
                        ))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        pipeline.run();
    }

    private static String getQuery(String table, String expirationDate) {
        String query = "SELECT * FROM " + table + " WHERE expiration_date = '" + expirationDate + "'";
        LOGGER.info("query={}", query);
        return query;
    }

    private static String toString(Object value) {
        return value instanceof Utf8 ? value.toString() : (String) value;
    }

    //@DefaultSchema(JavaFieldSchema.class)
    //@DefaultCoder(AvroCoder.class)
    public static class MySubscription implements Serializable {
            // avro schema from BQ converts timestamp to micro seconds
            private static final Schema TIMESTAMP_MICROS_LOGICAL_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
            //private static final Schema TIMESTAMP_MILLIS_LOGICAL_TYPE = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            private static final Schema DATE_LOGICAL_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            static Schema SCHEMA = SchemaBuilder.record("subscription").fields()
                    .requiredString("id")
                    .name("creation_timestamp").type(TIMESTAMP_MICROS_LOGICAL_TYPE).noDefault()
                    .name("expiration_date").type(DATE_LOGICAL_TYPE).noDefault()
                    .endRecord();

        //public static Schema SCHEMA = getSchemaUsingReflection();

        private static Schema getSchemaUsingReflection() {
            Schema schema = ReflectData.get().getSchema(MySubscription.class);
            String namespace = schema.getNamespace();
            return namespace != null && namespace.endsWith("$") ? schema : Schema.createRecord(schema.getFields());
        }

        private static Schema getSchemaFromResource(String schemaResourcePath)  {
            Schema.Parser parser = new Schema.Parser();
            try (InputStream inputStream = MySubscription.class.getClassLoader().getResourceAsStream(schemaResourcePath)) {
                return parser.parse(inputStream);
            } catch (IOException e) {
                String errorMessage = "Unable to parse avro schema: " + schemaResourcePath;
                LOGGER.error(errorMessage, e);
                throw new IllegalStateException(errorMessage);
            }
        }

        public String id;
        //@AvroSchema("{\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}")
        //@AvroSchema("{\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        public long creationTimestamp;

        //@AvroSchema("{\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}")
        public int expirationDate;

        public GenericRecord toGenericRecord() {
            GenericData.Record record = new GenericData.Record(SCHEMA);
            record.put("id", id);
            record.put("creation_timestamp", creationTimestamp);
            record.put("expiration_date", expirationDate);
            return record;
        }
        public static MySubscription fromGenericRecord(GenericRecord genericRecord) {
            MySubscription mySubscription = new MySubscription();
            mySubscription.id = asString(genericRecord.get("id"));
            mySubscription.creationTimestamp = (Long) genericRecord.get("creation_timestamp");
            mySubscription.expirationDate = (Integer) genericRecord.get("expiration_date");
            return mySubscription;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MySubscription that = (MySubscription) o;
            return creationTimestamp == that.creationTimestamp && expirationDate == that.expirationDate && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, creationTimestamp, expirationDate);
        }

        @Override
        public String toString() {
            return "MySubscription{id='" + id + '\'' + ", creationTimestamp=" + creationTimestamp + ", expirationDate=" + expirationDate + '}';
        }

        private static String asString(Object value) {
            return value instanceof Utf8 ? value.toString() : (String) value;
        }
    }
    
}

/*
bq rm bartek_dataset.mysubscription_table
>schema.json cat <<-EOF
[
  {
    "mode": "REQUIRED",
    "name": "id",
    "type": "STRING",
    "description" : "The id"
  },
  {
    "mode": "REQUIRED",
    "name": "creation_timestamp",
    "type": "TIMESTAMP",
    "description" : "The creation timestamp"
  },
  {
    "mode": "REQUIRED",
    "name": "expiration_date",
    "type": "DATE",
    "description" : "The expiration date"
  }
]
EOF
bq mk --table --description "bartek mysubscription table" --label owner:bartek bartek_dataset.mysubscription_table schema.json
rm schema.json
bq show --schema --format=prettyjson bartek_dataset.mysubscription_table
bq query --use_legacy_sql=false 'insert into bartek_dataset.mysubscription_table (id, creation_timestamp, expiration_date) values ("abc",CURRENT_TIMESTAMP(),CURRENT_DATE())'

gsutil cp terraform/MyBQReadWriteJob/mysubscription_table.csv gs://${BUCKET}/bigquery/


### Variables ###
PROJECT=$(gcloud config get-value project)
USER=bartek
EXPIRATION_DATE=2021-03-03
JOB_NAME=mybqreadwritejob
BUCKET=${PROJECT}-${USER}-${JOB_NAME}
gsutil mb gs://${BUCKET}


### Execute from maven  ###
mvn clean compile -DskipTests -Pdataflow-runner exec:java \
 -Dexec.mainClass=com.bawi.beam.dataflow.MyBQReadWriteJob \
 -Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --expirationDate=${EXPIRATION_DATE}"


### Create template from maven ###
mvn clean compile -DskipTests -Pdataflow-runner exec:java \
-Dexec.mainClass=com.bawi.beam.dataflow.MyBQReadWriteJob \
-Dexec.args="${JAVA_DATAFLOW_RUN_OPTS} \
 --runner=DataflowRunner \
 --stagingLocation=gs://${BUCKET}/staging \
 --templateLocation=gs://${BUCKET}/templates/${JOB_NAME}-template"


### Create template from java ###
mvn clean package -DskipTests -Pmake-dist -Pdataflow-runner
java -cp target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyBQReadWriteJob \
 ${JAVA_DATAFLOW_RUN_OPTS} \
 --runner=DataflowRunner \
 --stagingLocation=gs://${BUCKET}/staging \
 --templateLocation=gs://${BUCKET}/templates/${JOB_NAME}-template


### Execute from template ###
gcloud dataflow jobs run ${JOB_NAME}-${USER}-template-${EXPIRATION_DATE} \
  ${GCLOUD_DATAFLOW_RUN_OPTS} \
  --gcs-location gs://${BUCKET}/templates/${JOB_NAME}-template \
  --parameters expirationDate=${EXPIRATION_DATE}


 */
