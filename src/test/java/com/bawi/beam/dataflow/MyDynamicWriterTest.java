package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyDynamicWriterTest {
    private static final DateTimeFormatter Y_M_D_H_M_FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/mm");
    private static final Logger LOGGER = LoggerFactory.getLogger(MyDynamicWriterTest.class);

    private static final Schema SCHEMA = SchemaBuilder.record("record")
            .fields()
            .requiredString("body")
            .endRecord();

    static class MyFileNaming implements FileIO.Write.FileNaming {
        private final String path;

        public MyFileNaming(String path) {
            this.path = path;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            String filename = String.format("%s-currTs-%s-winMaxTs-%s-paneTiming-%s-shard-%s-of-%s%s", path, System.currentTimeMillis(), window.maxTimestamp().toString().replace(":","_").replace(" ","_"), pane.getTiming(), shardIndex, numShards, ".avro");
            LOGGER.info("Writing data to path='{}'", filename);
            return filename;
        }
    }

    @Test
    public void test() {
        // given
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(
                    KV.of(Y_M_D_H_M_FORMATTER.print(System.currentTimeMillis() - minutes(60)), "AA:Msg1hourOld"),
                    KV.of(Y_M_D_H_M_FORMATTER.print(System.currentTimeMillis() - minutes(1)), "VA:Msg1MinOld"),
                    KV.of(Y_M_D_H_M_FORMATTER.print(System.currentTimeMillis()), "AA:MsgCurrent")
                ))
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(GenericRecord.class)))
                                  .via(kv -> KV.of(kv.getValue().split(":")[0] + "/" + kv.getKey(), create(kv.getValue())))).setCoder(KvCoder.of(StringUtf8Coder.of(), AvroCoder.of(GenericRecord.class, SCHEMA)))
                .apply(Window.<KV<String, GenericRecord>>into(FixedWindows.of(Duration.standardSeconds(60)))
                        .withAllowedLateness(Duration.standardSeconds(60))
                        .triggering(Repeatedly.forever(AfterFirst.of(
                                AfterPane.elementCountAtLeast(2),
                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(60)))))
                        .discardingFiredPanes())
                .apply(FileIO.<String,KV<String, GenericRecord>>writeDynamic()
                        .by(KV::getKey)
                        .via(Contextful.fn(KV::getValue), AvroIO.<GenericRecord>sink(SCHEMA).withCodec(CodecFactory.fromString("snappy")))
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNaming(MyFileNaming::new)
                        .to("target/outputDir")
                        .withTempDirectory("target/tempDir")
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

    private int minutes(int n) {
        return 1000 * 60 * n;
    }

    private static GenericRecord create(String body) {
        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("body", body);
        return record;
    }
}
