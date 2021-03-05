package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class MyAvroReadWriteDataflowJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyAvroReadWriteDataflowJob.class);

    public interface PipelineOptions extends org.apache.beam.sdk.options.PipelineOptions {
        @Validation.Required
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> value);

        @Validation.Required
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> value);
    }


/*

### Run locally from ide using program args: ###
--output=src/test/resources/avro/persons.avro
--input=src/test/resources/avro/persons.avro


### Investigate avro file: ###
java -Dlog4j.configuration=file:src/main/resources/log4j.properties \
  -jar ~/Downloads/avro-tools-1.8.1.jar tojson src/test/resources/avro/persons.avro


### Run from maven ###
PROJECT=$(gcloud config get-value project)
JOB_NAME=myavroreadwritedataflowjob
BUCKET=${PROJECT}-$USER-${JOB_NAME}
gsutil mb gs://${BUCKET}

cd src/test/resources/avro ; gsutil cp persons.avro gs://${BUCKET}/input/persons.avro ; cd -
mvn clean compile -DskipTests exec:java \
-Pdataflow-runner \
-Dexec.mainClass=com.bawi.beam.dataflow.MyAvroReadWriteDataflowJob \
-Dexec.args="--project=${PROJECT} ${JAVA_DATAFLOW_RUN_OPTS} \
  --runner=DataflowRunner \
  --stagingLocation=gs://${BUCKET}/staging \
  --input=gs://${BUCKET}/input/persons.avro \
  --output=gs://${BUCKET}/output/persons.avro"


### Create template ###
PROJECT=$(gcloud config get-value project)
JOB_NAME=myavroreadwritedataflowjob
BUCKET=${PROJECT}-$USER-${JOB_NAME}
gsutil rm -r gs://${BUCKET}
gsutil mb gs://${BUCKET}
gsutil cp dataflow-templates/${JOB_NAME}-template_metadata gs://${BUCKET}/templates/${JOB_NAME}-template_metadata

mvn clean compile -DskipTests exec:java \
-Pdataflow-runner \
-Dexec.mainClass=com.bawi.beam.dataflow.MyAvroReadWriteDataflowJob \
-Dexec.args="--project=${PROJECT} ${JAVA_DATAFLOW_RUN_OPTS} \
 --runner=DataflowRunner \
 --stagingLocation=gs://${BUCKET}/staging \
 --templateLocation=gs://${BUCKET}/templates/${JOB_NAME}-template"


### Execute from template
PROJECT=$(gcloud config get-value project)
JOB_NAME=myavroreadwritedataflowjob
BUCKET=${PROJECT}-$USER-${JOB_NAME}
cd src/test/resources/avro ; gsutil cp persons.avro gs://${BUCKET}/input/persons.avro ; cd -

gcloud dataflow jobs run ${JOB_NAME} \
  --project=${PROJECT} ${GCLOUD_DATAFLOW_RUN_OPTS} \
  --gcs-location gs://${BUCKET}/templates/${JOB_NAME}-template \
  --parameters input=gs://${BUCKET}/input/persons.avro,output=gs://${BUCKET}/output/persons.avro

 */

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

/*        pipeline.apply(Create.of(new Person("Bob", 12), new Person("Alice", 19), new Person("Mike", null)))
                //.apply(Filter.by(p -> p.age != null && p.age >= 18))
                .apply(AvroIO.write(Person.class).to(options.getOutput())
                        .withoutSharding()); // to single file
        pipeline.run();*/

        ValueProvider<String> inputProvider = options.getInput();
        ValueProvider<String> outputProvider = options.getOutput();

        Schema schema = SchemaBuilder.record("person").fields().requiredString("name").optionalInt("age").endRecord();

        Counter counter = Metrics.counter(MyAvroReadWriteDataflowJob.class.getSimpleName(), "my-metrics-counter");
        Gauge gauge = Metrics.gauge(MyAvroReadWriteDataflowJob.class.getSimpleName(), "my-metrics-gauge");
        Distribution distribution = Metrics.distribution(MyAvroReadWriteDataflowJob.class.getSimpleName(), "my-metrics-distribution");

        pipeline.apply(AvroIO.read(Person.class).from(inputProvider))
                .apply("MapElements lambda", MapElements.into(TypeDescriptor.of(Person.class)).via(person -> {
                    counter.inc();
                    int nameLength = person.name.length();
                    distribution.update(nameLength);
                    gauge.set(nameLength);
                    LOGGER.info("MapElements lambda: inputProvider={}, output={}", inputProvider.get(), outputProvider.get());
                    return person;
                }))
                .apply("ParDo MyToGenericRecordFn", ParDo.of(new MyToGenericRecordFn(schema.toString(), inputProvider, outputProvider)))
                .setCoder(AvroGenericCoder.of(schema)) // required to explicitly set coder for GenericRecord
                .apply(AvroIO.writeGenericRecords(schema).to(outputProvider)
                    .withoutSharding());
        pipeline.run();
    }

    private static class MyToGenericRecordFn extends DoFn<Person, GenericRecord> {
        private String schemaString;
        private ValueProvider<String> inputProvider;
        private ValueProvider<String> outputProvider;
        private Schema schema;

        public MyToGenericRecordFn(String schemaString, ValueProvider<String> inputProvider, ValueProvider<String> outputProvider) {
            this.schemaString = schemaString;
            this.inputProvider = inputProvider;
            this.outputProvider = outputProvider;
        }

        @Setup
        public void init() {
            schema = new Schema.Parser().parse(schemaString);
        }

        @ProcessElement
        public void process(@Element Person person, OutputReceiver<GenericRecord> outputReceiver) {
            LOGGER.info("MyToGenericRecordFn process: input={}, output={} for person: {}", inputProvider.get(), outputProvider.get(), person);
            GenericData.Record genericRecord = new GenericData.Record(schema);
            genericRecord.put("name", person.name);
            genericRecord.put("age", person.age);
            outputReceiver.output(genericRecord);
        }
    }

    @DefaultSchema(JavaFieldSchema.class)
    static class Person {
        public String name;
        @Nullable public Integer age;
        Person() { }
        Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return Objects.equals(name, person.name) &&
                    Objects.equals(age, person.age);
        }
        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
        @Override
        public String toString() {
            return "Person{name='" + name + ", age=" + age + '}';
        }
    }
}
