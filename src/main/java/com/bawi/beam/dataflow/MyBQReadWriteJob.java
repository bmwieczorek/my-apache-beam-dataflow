package com.bawi.beam.dataflow;

import com.bawi.beam.dataflow.schema.AvroToBigQuerySchemaConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;


public class MyBQReadWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBQReadWriteJob.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/mm");

    public interface MyBQOptions extends PipelineOptions {
//    public interface MyBQOptions extends DataflowPipelineOptions {
        @Validation.Required
        ValueProvider<String> getExpirationDate();
        void setExpirationDate(ValueProvider<String> value);

        @Validation.Required
//        @Default.String("bartek_mybqreadwritejob.mysubscription_table")
        String getTableSpec();
        void setTableSpec(String value);

//        @Validation.Required
//        ValueProvider<String> getOutputPath();
//        void setOutputPath(ValueProvider<String>  value);

//        @Validation.Required
//        ValueProvider<String> getTempPath();
//        void setTempPath(ValueProvider<String>  value);

    }
    
    public static void main(String[] args) {
//        System.out.println(MySubscription.SCHEMA);
//        System.setProperty("java.util.logging.config.file", "src/main/resources/logging.properties");
        //  bq --location=US mk --dataset bartek_mybqreadwritejob
        //  bq rm -r -f -d bartek_mybqreadwritejob

        args = PipelineUtils.updateArgsWithDataflowRunner(args
//                , "--jobName=bartek-mybqreadwritejob-2021-03-03-flexrs",
//                "--maxNumWorkers=10", "--workerMachineType=n1-standard-2", "--flexRSGoal=COST_OPTIMIZED",
//                "--expirationDate=2021-03-03", "--tableSpec=bartek_mybqreadwritejob.mysubscription_table"
        );
        MyBQOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyBQOptions.class);
//        DataflowPipelineOptions pipelineOptions =
//              DataflowRunnerUtils.createDataflowRunnerOptions(PipelineOptionsFactory.fromArgs(args).withValidation().as(MyBQOptions.class), MyBQOptions.class.getSimpleName());

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // requires org.apache.beam:beam-sdks-java-io-google-cloud-platform
        String tableSpec = pipelineOptions.getTableSpec();

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
                        .fromQuery(ValueProvider.NestedValueProvider.of(pipelineOptions.getExpirationDate(), expirationDate -> getQuery(tableSpec, expirationDate)))
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
                        .to(tableSpec)
                        .useAvroLogicalTypes()
                        .withSchema(AvroToBigQuerySchemaConverter.convert(MySubscription.SCHEMA))
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

//                .apply(MapElements
//                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(GenericRecord.class)))
//                        .via(genericRecord ->
//                                {
//                                    LOGGER.info("genericRecord = {}", genericRecord);
//                                    Long creationTimestampMicros = (Long) genericRecord.get("creation_timestamp");
//                                    LOGGER.info("creationTimestampMicros = {}", creationTimestampMicros);
//                                    long creationTimestampMillis = creationTimestampMicros / 1000;
//                                    LOGGER.info("creationTimestampMillis = {}", creationTimestampMillis);
//                                    String path = FORMATTER.print(creationTimestampMillis);
//                                    LOGGER.info("path = {}", path);
//                                    return KV.of(path, genericRecord);
//                                }
//                        )
//                )
//                .setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GenericRecord.class, MySubscription.SCHEMA)))
//
//                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(60))))
//                .apply(FileIO.<String, KV<String, GenericRecord>>writeDynamic()
//                        .by(KV::getKey)
//                        .via(Contextful.fn(KV::getValue), ParquetIO.<GenericRecord>sink(MySubscription.SCHEMA).withCompressionCodec(CompressionCodecName.SNAPPY))
////                        .via(Contextful.fn(KV::getValue), AvroIO.<GenericRecord>sink(MySubscription.SCHEMA).withCodec(CodecFactory.fromString("snappy")))
//                        .withDestinationCoder(StringUtf8Coder.of())
//                        .withNaming(path -> new MyFileNaming(path, "snappy", ".parquet"))
//                        .to(pipelineOptions.getOutputPath())
//                        .withTempDirectory(pipelineOptions.getTempPath())
//                        .withNumShards(1000));


        pipeline.run();
    }

//    static class MyFileNaming implements FileIO.Write.FileNaming {
//        private String path;
//        private String compression;
//        private String extension;
//
//        public MyFileNaming(String path, String compression, String extension) {
//            this.path = path;
//            this.compression = compression;
//            this.extension = extension;
//        }
//
//        @Override
//        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
//            String filename = String.format("%s-currTs-%s-winMaxTs-%s-paneTiming-%s-shard-%s-of-%s-%s%s",
//                    path, System.currentTimeMillis(), window.maxTimestamp().toString().replace(":","_").replace(" ","_"), pane.getTiming(), shardIndex, numShards, this.compression, this.extension);
//            LOGGER.info("Writing data to path='{}'", filename);
//            return filename;
//        }
//    }

    private static String getQuery(String tableSpec, String expirationDate) {
        String query = "SELECT * FROM " + tableSpec + " WHERE expiration_date = '" + expirationDate + "'";
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

        public static class MyRequiredSubRecord implements Serializable{
            public int myRequiredInt;
            public Long myNullableLong;
            public boolean myRequiredBoolean;

            public static MyRequiredSubRecord fromGenericRecord(GenericRecord genericRecord) {
                MyRequiredSubRecord myRequiredSubRecord = new MyRequiredSubRecord();
                myRequiredSubRecord.myRequiredInt = ((Long) genericRecord.get("myRequiredInt")).intValue();
                myRequiredSubRecord.myNullableLong = (Long) genericRecord.get("myNullableLong");
                myRequiredSubRecord.myRequiredBoolean = (boolean) genericRecord.get("myRequiredBoolean");
                return myRequiredSubRecord;
            }

            public GenericRecord toGenericRecord() {
                GenericData.Record record = new GenericData.Record(SCHEMA.getField("myRequiredSubRecord").schema());
                record.put("myRequiredInt", myRequiredInt);
                record.put("myNullableLong", myNullableLong);
                record.put("myRequiredBoolean", myRequiredBoolean);
                return record;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MyRequiredSubRecord that = (MyRequiredSubRecord) o;
                return myRequiredInt == that.myRequiredInt &&
                        myRequiredBoolean == that.myRequiredBoolean &&
                        Objects.equals(myNullableLong, that.myNullableLong);
            }

            @Override
            public int hashCode() {
                return Objects.hash(myRequiredInt, myNullableLong, myRequiredBoolean);
            }

            @Override
            public String toString() {
                return "MyRequiredSubRecord{" +
                        "myRequiredInt=" + myRequiredInt +
                        ", myNullableLong=" + myNullableLong +
                        ", myRequiredBoolean=" + myRequiredBoolean +
                        '}';
            }
        }

        public static class MyOptionalArraySubRecord implements Serializable{
            public double myRequiredDouble;
            public Float myOptionalFloat;

            public static MyOptionalArraySubRecord fromGenericRecord(GenericRecord genericRecord) {
                MyOptionalArraySubRecord myOptionalArraySubRecord = new MyOptionalArraySubRecord();
                myOptionalArraySubRecord.myRequiredDouble = (Double) genericRecord.get("myRequiredDouble");
                myOptionalArraySubRecord.myOptionalFloat = ((Double) genericRecord.get("myRequiredDouble")).floatValue();
                return myOptionalArraySubRecord;
            }

            public GenericRecord toGenericRecord() {
                Schema myOptionalArraySubRecordsSchema = unwrapUnion(SCHEMA.getField("myOptionalArraySubRecords").schema());
                GenericData.Record record = new GenericData.Record(myOptionalArraySubRecordsSchema.getElementType());
                record.put("myRequiredDouble", myRequiredDouble);
                record.put("myOptionalFloat", myOptionalFloat);
                return record;
            }


            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MyOptionalArraySubRecord that = (MyOptionalArraySubRecord) o;
                return Double.compare(that.myRequiredDouble, myRequiredDouble) == 0 &&
                        Objects.equals(myOptionalFloat, that.myOptionalFloat);
            }

            @Override
            public int hashCode() {
                return Objects.hash(myRequiredDouble, myOptionalFloat);
            }

            @Override
            public String toString() {
                return "MyOptionalArraySubRecord{" +
                        "myRequiredDouble=" + myRequiredDouble +
                        ", myOptionalFloat=" + myOptionalFloat +
                        '}';
            }
        }

        private static Schema unwrapUnion(Schema unionSchema) {
            List<Schema> types = unionSchema.getTypes();
            return types.get(0).getType() != Schema.Type.NULL ? types.get(0) : types.get(1);
        }

        public String id;
        //@AvroSchema("{\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}") // cannot use timestamp-millis for reading
        //@AvroSchema("{\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}")
        public long creationTimestamp;

        //@AvroSchema("{\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}")
        public int expirationDate;

        public BigDecimal myNumeric;

        public List<Long> numbers;

        public MyRequiredSubRecord myRequiredSubRecord;

        public List<MyOptionalArraySubRecord> myOptionalArraySubRecords;

        public GenericRecord toGenericRecord() {
            GenericData.Record record = new GenericData.Record(SCHEMA);
            record.put("id", id);
            record.put("creation_timestamp", creationTimestamp);
            record.put("expiration_date", expirationDate);

            double value = 1.234d;
            BigDecimal bigDecimal = BigDecimal.valueOf(value).setScale(getDecimalScale(record, "my_numeric"), RoundingMode.UNNECESSARY);

//            record.put("my_numeric", null);
            record.put("my_numeric", new Random().nextBoolean() ?
                    ByteBuffer.wrap(myNumeric.unscaledValue().toByteArray()) :
                    ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray()));

            ArrayList<Long> listOnlyWithNull = new ArrayList<>();
            listOnlyWithNull.add(null);

            ArrayList<Long> listWithANull = new ArrayList<>();
            listWithANull.add(123L);
            listWithANull.add(null);
            listWithANull.add(567L);

            record.put("numbers", new Random().nextBoolean() ? listOnlyWithNull : listWithANull);

            record.put("myRequiredSubRecord", myRequiredSubRecord.toGenericRecord());

            List<GenericRecord> genericRecords = new ArrayList<>();
            for (MyOptionalArraySubRecord myOptionalArraySubRecord : myOptionalArraySubRecords) {
                genericRecords.add(myOptionalArraySubRecord.toGenericRecord());
            }

            Schema myOptionalArraySubRecordsSchema = unwrapUnion(SCHEMA.getField("myOptionalArraySubRecords").schema());
            GenericData.Array<GenericRecord> myOptionalArraySubRecords = new GenericData.Array<>(myOptionalArraySubRecordsSchema, genericRecords);

            record.put("myOptionalArraySubRecords", myOptionalArraySubRecords);

            LOGGER.info("Created {}", record);
            return record;
        }

        private int getDecimalScale(GenericData.Record record, String fieldName) {
            Schema myNumericSchemaUnion = record.getSchema().getField(fieldName).schema();
            Schema myNumericSchema = unwrapUnion(myNumericSchemaUnion);
            LogicalTypes.Decimal myNumericSchemaLogicalType = (LogicalTypes.Decimal) myNumericSchema.getLogicalType();
            return myNumericSchemaLogicalType.getScale();
        }

        public static MySubscription fromGenericRecord(GenericRecord genericRecord) {
            MySubscription mySubscription = new MySubscription();
            mySubscription.id = asString(genericRecord.get("id"));
            mySubscription.creationTimestamp = (Long) genericRecord.get("creation_timestamp");
            mySubscription.expirationDate = (Integer) genericRecord.get("expiration_date");
            mySubscription.myNumeric = getBigDecimal(genericRecord, "my_numeric");
            @SuppressWarnings("unchecked")
            List<Long> numbers = (List<Long>) genericRecord.get("numbers");
            mySubscription.numbers = numbers == null ? null : new ArrayList<>(numbers);

            GenericData.Record myRequiredSubRecord = (GenericData.Record) genericRecord.get("myRequiredSubRecord");
            mySubscription.myRequiredSubRecord = MyRequiredSubRecord.fromGenericRecord(myRequiredSubRecord);

            List<MyOptionalArraySubRecord> myOptionalArraySubRecords = new ArrayList<>();
            @SuppressWarnings("unchecked")
            GenericData.Array<GenericRecord> myOptionalArraySubGenericRecords = (GenericData.Array<GenericRecord>) genericRecord.get("myOptionalArraySubRecords");
            for (GenericRecord myOptionalArraySubGenericRecord : myOptionalArraySubGenericRecords) {
                myOptionalArraySubRecords.add(MyOptionalArraySubRecord.fromGenericRecord(myOptionalArraySubGenericRecord));
            }
            mySubscription.myOptionalArraySubRecords = myOptionalArraySubRecords;

            LOGGER.info("Created {}", mySubscription);
            return mySubscription;
        }

        private static BigDecimal getBigDecimal(GenericRecord genericRecord, String fieldName) {
            Schema myNumericSchema = genericRecord.getSchema().getField(fieldName).schema();
            LogicalTypes.Decimal myNumericSchemaLogicalType = (LogicalTypes.Decimal) unwrapUnion(myNumericSchema).getLogicalType();
            int scale = myNumericSchemaLogicalType.getScale();
            ByteBuffer byteBuffer = (ByteBuffer) genericRecord.get(fieldName);
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            BigInteger bigInteger = new BigInteger(bytes);
            BigDecimal bigDecimal = new BigDecimal(bigInteger, scale);
            return bigDecimal;
        }

        private static String asString(Object value) {
            return value instanceof Utf8 ? value.toString() : (String) value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MySubscription that = (MySubscription) o;
            return creationTimestamp == that.creationTimestamp &&
                    expirationDate == that.expirationDate &&
                    Objects.equals(id, that.id) &&
                    Objects.equals(myNumeric, that.myNumeric) &&
                    Objects.equals(numbers, that.numbers) &&
                    Objects.equals(myRequiredSubRecord, that.myRequiredSubRecord) &&
                    Objects.equals(myOptionalArraySubRecords, that.myOptionalArraySubRecords);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, creationTimestamp, expirationDate, myNumeric, numbers, myRequiredSubRecord, myOptionalArraySubRecords);
        }

        @Override
        public String toString() {
            return "MySubscription{" +
                    "id='" + id + '\'' +
                    ", creationTimestamp=" + creationTimestamp +
                    ", expirationDate=" + expirationDate +
                    ", myNumeric=" + myNumeric +
                    ", numbers=" + numbers +
                    ", myRequiredSubRecord=" + myRequiredSubRecord +
                    ", myOptionalArraySubRecords=" + myOptionalArraySubRecords +
                    '}';
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
OWNER=bartek
EXPIRATION_DATE=2021-03-03
JOB=mybqreadwritejob
BUCKET=${PROJECT}-${OWNER}-${JOB}
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
gcloud dataflow jobs run ${JOB}-${OWNER}-template-${EXPIRATION_DATE} \
  ${GCLOUD_DATAFLOW_RUN_OPTS} \
  --gcs-location gs://${BUCKET}/templates/${JOB}-template \
  --parameters expirationDate=${EXPIRATION_DATE}


 */
