package com.bawi.beam.dataflow;

import com.google.bigtable.v2.*;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class MyBigtableWriteReadTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyBigtableWriteReadTest.class);

    public interface MyBigtablePipelineOptions extends PipelineOptions {
        @Default.String("my-instance")
        @Validation.Required
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> value);

        @Default.String("my-table")
        @Validation.Required
        ValueProvider<String> getTableId();
        void setTableId(ValueProvider<String> value);

    }

    @Test
    public void test() throws IOException {
        String project = System.getenv("GCP_PROJECT");
        String[] args = {"--project=" + project, "--tableId=my-table"};
        MyBigtablePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(MyBigtablePipelineOptions.class);

//        Pipeline writePipeline = TestPipeline.create(options);
//        PCollection<KV<ByteString, Iterable<Mutation>>> input =
//                writePipeline.apply(Create.of(
//                        createBigTableKV("myKey1", List.of("abc11", "def11")),
//                        createBigTableKV("myKey2", List.of("abc22")),
//                        createBigTableKV("myKey2", List.of("def22")),
//                        createBigTableKV("myKey3", List.of("abc33")),
//                        createBigTableKV("myKey4", List.of("abc44"))
//                ).withCoder(getCoder()));
//        input.apply(BigtableIO.write().withProjectId(project).withInstanceId(options.getInstanceId()).withTableId(options.getTableId()));
//        writePipeline.run().waitUntilFinish();


        Pipeline readPipeline = TestPipeline.create(options);
        readPipeline
                .apply(BigtableIO.read()
                        .withProjectId(project)
                        .withInstanceId(options.getInstanceId())
                        .withTableId(options.getTableId())
//                        .withRowFilter(RowFilter.newBuilder().setFamilyNameRegexFilter("my-column-family1.*").build())
//                        .withRowFilter(RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8("my-column-qualifier1.*")).build())
                        .withRowFilter(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build())
//                        .withKeyRange(ByteKeyRange.of(ByteKey.copyFrom("myKey1".getBytes()), ByteKey.copyFrom("myKey3".getBytes()))) // returns rows with key1 and key2 (not key3)
//                        .withKeyRange(ByteKeyRange.ALL_KEYS.withStartKey(ByteKey.copyFrom("myKey2".getBytes()))) // all keys starting with key2 (inc) onwards
                )
                .apply(MapElements.into(TypeDescriptors.voids()).via(row -> {
                    ByteString key = row.getKey();
                    List<Family> families = row.getFamiliesList();
                    families.forEach(family -> {
                        List<Column> columns = family.getColumnsList();
                        columns.forEach(column -> {
                            List<Cell> cells = column.getCellsList();
                            cells.forEach(cell -> LOGGER.info("key={},family={},column={},cell={}",
//                                    key.toStringUtf8(), family, column, cell));
                                    key.toStringUtf8(), family.getName(), column.getQualifier().toStringUtf8(), cell));
                        });
                    });
                    return null;
                }));
        readPipeline.run().waitUntilFinish();

    }

    private static KvCoder<ByteString, Iterable<Mutation>> getCoder() {
        return KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)));
    }

    private static KV<ByteString, Iterable<Mutation>> createBigTableKV(String keyName, List<String> cellTextList) {
        ByteString key = ByteString.copyFromUtf8(keyName);
        ImmutableList.Builder<Mutation> mutationListBuilder = ImmutableList.builder();
        for (int i = 0; i < cellTextList.size(); i++) {
            Mutation mutation = Mutation.newBuilder().setSetCell(createCell(cellTextList.get(i), "my-column-qualifier" + i)).build();
            mutationListBuilder.add(mutation).build();
        }
        ImmutableList<Mutation> mutations = mutationListBuilder.build();
        return KV.of(key, mutations);
    }

    private static Mutation.SetCell createCell(String text, String columnQualifier) {
        return Mutation.SetCell.newBuilder()
                .setFamilyName("my-column-family1")
                .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                .setTimestampMicros(System.currentTimeMillis() * 1000)
                .setValue(ByteString.copyFromUtf8(text))
                .build();
    }
}


// cbt createinstance my-instance my-instance-name my-cluster us-central1-a 1 HDD
// echo project = `gcloud config get-value project` > ~/.cbtrc
// echo instance = my-instance >> ~/.cbtrc
// cbt createtable my-table
// cbt createfamily my-table my-column-family1
// cbt set my-table row1 my-column-family1:my-column-qualifier1=test-value
// cbt read my-table
//myKey1
//        my-column-family1:my-column-qualifier0   @ 2022/11/23-14:32:20.953000
//        "abc11"
//        my-column-family1:my-column-qualifier0   @ 2022/11/23-14:22:37.980000
//        "abc1"
//        my-column-family1:my-column-qualifier1   @ 2022/11/23-14:32:20.953000
//        "def11"
//        my-column-family1:my-column-qualifier1   @ 2022/11/23-14:22:37.980000
//        "def1"
