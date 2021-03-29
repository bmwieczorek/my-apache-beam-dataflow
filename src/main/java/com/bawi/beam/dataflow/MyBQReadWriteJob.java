package com.bawi.beam.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;


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
//        System.out.println(MySubscription.SCHEMA);
//        System.setProperty("java.util.logging.config.file", "src/main/resources/logging.properties");

        MyBQOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyBQOptions.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        String table = pipelineOptions.getTable();

        //noinspection Convert2Lambda // to infer type from anonymous class runtime types (not to use .setCoder)
        pipeline.apply(BigQueryIO.read(new SerializableFunction<SchemaAndRecord, MySubscription>() { //
                            @Override
                            public MySubscription apply(SchemaAndRecord tableSchemaAndGenericRecord) {
                                GenericRecord genericRecord = tableSchemaAndGenericRecord.getRecord();
                                // avro schema from BQ converts timestamp to micro seconds
                                MySubscription mySubscription = MySubscription.fromGenericRecord(genericRecord); // timestamp is in micro secs
                                LOGGER.info("Read {}, with schema {}", mySubscription, tableSchemaAndGenericRecord.getTableSchema());
                                return mySubscription;
                            }
                        })
                        //.from(pipelineOptions.getTableName()))  // all data
                        .fromQuery(ValueProvider.NestedValueProvider.of(pipelineOptions.getExpirationDate(), expirationDate -> getQuery(table, expirationDate)))
                        // non default settings below:
                        .useAvroLogicalTypes() // convert BQ TIMESTAMP to avro long millis/micros and BQ DATE to avro int
                        .withoutValidation() // skip validation if using value provider for query
                        .usingStandardSql() // required for TIMESTAMP function - needs to be below .fromQuery
                        .withTemplateCompatibility() // required to re-run jobs from templates
                        //.withCoder(SerializableCoder.of(MySubscription.class)) // or annotate class with @DefaultCoder(SerializableCoder.class) while using anonymous class SerializableFunction (instead of lambda) to infer the type
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
                        .withAvroSchemaFactory(qTableSchema -> MySubscription.SCHEMA)
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

    // @DefaultSchema(JavaFieldSchema.class)
    @DefaultCoder(SerializableCoder.class) // or @DefaultCoder(AvroCoder.class), it requires anonymous SerializableFunction (not lambda) or use .withCoder(SerializableCoder.of(MySubscription.class))
    public static class MySubscription implements Serializable {
            // when reading with logical types enabled BQ internally converts BQ TIMESTAMP into avro long timestamp-micros logical type
//            private static final Schema TIMESTAMP_MICROS_LOGICAL_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
//            private static final Schema DATE_LOGICAL_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            static Schema SCHEMA =
                 getSchemaFromResource("schema/MyBQReadWriteJob.avsc");

//                SchemaBuilder.record("subscription").doc("My Subscription record").fields()
//                    .requiredString("id")
//                    .name("creation_timestamp").type(TIMESTAMP_MICROS_LOGICAL_TYPE).noDefault() // needs to be timestamp_micros (not timestamp_millis)
//                    .name("expiration_date").doc("Expiration date field").type(DATE_LOGICAL_TYPE).noDefault()
//                .endRecord();

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

INSERT INTO bartek_dataset.mysubscription_table (id,creation_timestamp, expiration_date) values("abc",TIMESTAMP("2021-03-03 03:03:03+00"),DATE '2021-03-03');
select * from bartek_dataset.mysubscription_table;


### Variables ###
PROJECT=$(gcloud config get-value project)
USER=bartek
EXPIRATION_DATE=2021-03-03
JOB=mybqreadwritejob
BUCKET=${PROJECT}-${USER}-${JOB}
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
 --templateLocation=gs://${BUCKET}/templates/${JOB}-template"


### Create template from java ###
mvn clean package -DskipTests -Pmake-dist -Pdataflow-runner
java -cp target/my-apache-beam-dataflow-0.1-SNAPSHOT.jar com.bawi.beam.dataflow.MyBQReadWriteJob \
 ${JAVA_DATAFLOW_RUN_OPTS} \
 --runner=DataflowRunner \
 --stagingLocation=gs://${BUCKET}/staging \
 --templateLocation=gs://${BUCKET}/templates/${JOB}-template


### Execute from template ###
gcloud dataflow jobs run ${JOB}-${USER}-template-${EXPIRATION_DATE} \
  ${GCLOUD_DATAFLOW_RUN_OPTS} \
  --gcs-location gs://${BUCKET}/templates/${JOB}-template \
  --parameters expirationDate=${EXPIRATION_DATE}


 */
