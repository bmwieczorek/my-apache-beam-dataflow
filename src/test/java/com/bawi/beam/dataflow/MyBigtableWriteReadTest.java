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
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyBigtableWriteReadTest {

    @Before
    public void setup() throws IOException, InterruptedException {
        runBashProcess("cbt createfamily my-table " + OPTIONAL_KEY_CF);
        runBashProcess("cbt createfamily my-table " + VALUE_CF);


        Pipeline writePipeline = TestPipeline.create(OPTIONS);
        Instant now = Instant.now();
        PCollection<KV<ByteString, Iterable<Mutation>>> input =
                writePipeline.apply(Create.of(
//                        RQ_KEY                           OPTIONAL_KEY_CF:RQ_CODE,RQ_SERVICE,RQ_TS  VALUE_CF:ID,SUB_ID,
//                        aacc#9223370366594304660#AcSG    optional_key_cf:A1C1,SERV2                value_cf:ac1,1

                        // most recent matching
                        cbtKV("aabb#" + (Long.MAX_VALUE - now.toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "ab1", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + (Long.MAX_VALUE - now.plusMillis(2000).toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "a2", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + (Long.MAX_VALUE - now.plusMillis(1000).toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "ab3", SUB_ID_CQ, "1")),

                        // by rq_id
                        cbtKV("aabb#" + (Long.MAX_VALUE - now.plusMillis(3000).toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Map.of(RQ_ID_CQ, "abc123"),
                                VALUE_CF,      Map.of(ID_CQ, "ab4", SUB_ID_CQ, "1")),

                        // most recent by code and service
                        cbtKV("aabb#" + (Long.MAX_VALUE - now.plusMillis(4000).toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1B1", SERVICE_CQ, "Serv1"),
                                VALUE_CF,      Map.of(ID_CQ, "ab5", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + (Long.MAX_VALUE - now.plusMillis(5000).toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1B1", SERVICE_CQ, "Serv1"),
                                VALUE_CF,      Map.of(ID_CQ, "ab6", SUB_ID_CQ, "1")),

                        cbtKV("aacc#" + (Long.MAX_VALUE - now.toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1C1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ac1", SUB_ID_CQ, "1")),

                        cbtKV("aadd#" + (Long.MAX_VALUE - now.toEpochMilli()) + "#" + randomString(),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1D1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ad1", SUB_ID_CQ, "1"))


                ).withCoder(getCoder()));
        input.apply(BigtableIO.write().withProjectId(PROJECT).withInstanceId(OPTIONS.getInstanceId()).withTableId(OPTIONS.getTableId()));
        writePipeline.run().waitUntilFinish();

//        runBashProcess("cbt read my-table");

    }


    @Test
    public void testByKey() {
        Pipeline readPipeline = TestPipeline.create(OPTIONS);

        /*
        RowFilter build = RowFilter.newBuilder().setFamilyNameRegexFilter("my-column-family1.*").build();
        ByteKeyRange r1 = ByteKeyRange.of(ByteKey.copyFrom("myKey1".getBytes()), ByteKey.copyFrom("myKey1_".getBytes()));
        ByteKeyRange r2 = ByteKeyRange.of(ByteKey.copyFrom("myKey2".getBytes()), ByteKey.copyFrom("myKey4".getBytes()));

         */
        readPipeline
                .apply(BigtableIO.read()
                        .withProjectId(PROJECT)
                        .withInstanceId(OPTIONS.getInstanceId())
                        .withTableId(OPTIONS.getTableId())
                        .withKeyRange(ByteKeyRange.of(ByteKey.copyFrom("aacc#".getBytes()), ByteKey.copyFrom("aacc#~".getBytes()))) // returns rows with key1 and key2 (not key3)
//                        .withRowFilter(RowFilter.newBuilder().setCondition(condition).build())

                        // AC|AC|AC|AC#20221008|20221008|20221009|20221014#8144|358|447|139
//                        .withKeyRanges(List.of(r1, r2)) // returns row for key1, key2, key3
//                        .withKeyRange(ByteKeyRange.of(ByteKey.copyFrom("myKey1".getBytes()), ByteKey.copyFrom("myKey3".getBytes()))) // returns rows with key1 and key2 (not key3)
//                        .withKeyRange(ByteKeyRange.ALL_KEYS.withStartKey(ByteKey.copyFrom("myKey2".getBytes()))) // all keys starting with key2 (inc) onwards

//                        .withRowFilter(RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8("my-column-qualifier1.*")).build())

//                        .withRowFilter(
//                                RowFilter.newBuilder().setInterleave(
//                                        RowFilter.Interleave.newBuilder().addAllFilters(
//                                                List.of(
//                                                        RowFilter.newBuilder().setFamilyNameRegexFilter(OPT_KEY_CF1 + ".*").build()
//                                                )
//                                        ).build()
//                                ).build()
//                        )

//                                .withRowFilter(RowFilter.newBuilder().setChain(chain).build())

                        /*
                        // chained filter (all applied)
                        .withRowFilter(RowFilter.newBuilder().setChain(
                                            RowFilter.Chain.newBuilder().addAllFilters(
                                                List.of(
                                                    RowFilter.newBuilder().setFamilyNameRegexFilter(OPT_KEY_CF1 + ".*").build(),
//                                                    RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8(CODE_CQ + ".*")).build(),
//                                                    RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build(), // latest version based on timestamp
//                                                    RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFromUtf8(".*1$")).build()
                                                )
                                            )
                                       ).build())
                         */
                )
                .apply(MapElements.into(TypeDescriptors.voids()).via(row -> {
                    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
                    ByteString key = row.getKey();
                    List<Family> families = row.getFamiliesList();
                    families.forEach(family -> {
                        List<Column> columns = family.getColumnsList();
                        columns.forEach(column -> {
                            List<Cell> cells = column.getCellsList();
                            cells.forEach(cell -> {
                                if (OPTIONAL_KEY_CF.equals(family.getName())
                                        && (RQ_ID_CQ.equals(column.getQualifier().toStringUtf8()))
                                        && ("abc123".equals(cell.getValue().toStringUtf8()))
                                ) {
//                                    LOGGER.info("by rq_iq:  key={},family={},column={},cell={}",
//                                            //                                    key.toStringUtf8(), family, column, cell));
//                                            key.toStringUtf8(), family.getName(), column.getQualifier().toStringUtf8(), cell);
                                    atomicBoolean.set(true);
                                }
                            });
                        });
                    });
//                    if (atomicBoolean.get()) {
//                        LOGGER.info("by rq_iq:  row={}", row);
//                        atomicBoolean.set(false);
//                    }
                    LOGGER.info("by key: row={}", row);
                    return null;
                }));
        readPipeline.run().waitUntilFinish();
    }

    @Test
    public void testByKeyAndValueCondition() {
        Pipeline readPipeline = TestPipeline.create(OPTIONS);

        RowFilter.Chain chain = RowFilter.Chain.newBuilder().addAllFilters(
                List.of(
                        RowFilter.newBuilder().setFamilyNameRegexFilter(OPTIONAL_KEY_CF).build(),
                        RowFilter.newBuilder().setColumnQualifierRegexFilter(ByteString.copyFromUtf8(RQ_ID_CQ)).build(),
                        RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFromUtf8("abc123")).build()
                )
        ).build();

        RowFilter passAllFilter = RowFilter.newBuilder().setPassAllFilter(true).build();
        RowFilter blockAllFilter = RowFilter.newBuilder().setBlockAllFilter(true).build();
        RowFilter rowFilter = RowFilter.newBuilder().setChain(chain).build();
        RowFilter.Condition condition = RowFilter.Condition.newBuilder().setPredicateFilter(rowFilter).setTrueFilter(passAllFilter).setFalseFilter(blockAllFilter).build();


        readPipeline
                .apply(BigtableIO.read()
                                .withProjectId(PROJECT)
                                .withInstanceId(OPTIONS.getInstanceId())
                                .withTableId(OPTIONS.getTableId())
                                .withKeyRange(ByteKeyRange.of(ByteKey.copyFrom("aabb#".getBytes()), ByteKey.copyFrom("aabb#~".getBytes()))) // returns rows with key1 and key2 (not key3)
                        .withRowFilter(RowFilter.newBuilder().setCondition(condition).build())

                )
                .apply(MapElements.into(TypeDescriptors.voids()).via(row -> {
                    ByteString key = row.getKey();
                    List<Family> families = row.getFamiliesList();
                    families.forEach(family -> {
                        List<Column> columns = family.getColumnsList();
                        columns.forEach(column -> {
                            List<Cell> cells = column.getCellsList();
                            cells.forEach(cell -> {
//                                LOGGER.info("by rq_iq:  key={},family={},column={},cell={}", key.toStringUtf8(), family.getName(), column.getQualifier().toStringUtf8(), cell);
                            });
                        });
                    });
                    LOGGER.info("by key: row={}", row);
                    return null;
                }));
        readPipeline.run().waitUntilFinish();
    }

    @After
    public void destroy() throws IOException, InterruptedException {
        runBashProcess("cbt deletefamily my-table " + OPTIONAL_KEY_CF);
        runBashProcess("cbt deletefamily my-table " + VALUE_CF);
    }

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final  MyBigtablePipelineOptions OPTIONS = createPipelineOptions(PROJECT);

    private static final Logger LOGGER = LoggerFactory.getLogger(MyBigtableWriteReadTest.class);
    public static final String OPTIONAL_KEY_CF = "optional_key_cf";
    public static final String RQ_ID_CQ = "rq_id_cq";
    public static final String CODE_CQ = "code_cq";
    public static final String SERVICE_CQ = "service_cq";

    public static final String VALUE_CF = "value_cf";
    public static final String ID_CQ = "id_cq";
    public static final String SUB_ID_CQ = "sub_id_cq";


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

    private static MyBigtablePipelineOptions createPipelineOptions(String project) {
        String[] args = {"--project=" + project, "--tableId=my-table"};
//        args = DataflowUtils.updateDataflowArgs(args);
        return PipelineOptionsFactory.fromArgs(args).as(MyBigtablePipelineOptions.class);
    }

    private static String randomString() {
        return RandomStringUtils.randomAlphanumeric(4);
    }

    private static int randomInt() {
        return new Random().nextInt(10);
    }

    private static long rTs() {
        return Long.MAX_VALUE - System.currentTimeMillis();
    }

    private static KvCoder<ByteString, Iterable<Mutation>> getCoder() {
        return KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)));
    }


    private static KV<ByteString, Iterable<Mutation>> cbtKV(String rowKey, String columnFamily, Map<String, String> columnQualifierAndValueList) {
        ByteString key = ByteString.copyFromUtf8(rowKey);
        ImmutableList.Builder<Mutation> mutationListBuilder = ImmutableList.builder();
        columnQualifierAndValueList.forEach((columnQualifier, value) -> {
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setTimestampMicros(System.currentTimeMillis() * 1000)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutationListBuilder.add(mutation);
        });
        ImmutableList<Mutation> mutations = mutationListBuilder.build();
        return KV.of(key, mutations);
    }

    private static KV<ByteString, Iterable<Mutation>> cbtKV(String rowKey,
                                                            String columnFamily1, Map<String, String> columnQualifierAndValueList1,
                                                            String columnFamily2, Map<String, String> columnQualifierAndValueList2) {
        ByteString key = ByteString.copyFromUtf8(rowKey);
        ImmutableList.Builder<Mutation> mutationListBuilder = ImmutableList.builder();
        columnQualifierAndValueList1.forEach((columnQualifier, value) -> {
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily1)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setTimestampMicros(System.currentTimeMillis() * 1000)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutationListBuilder.add(mutation);
        });
        columnQualifierAndValueList2.forEach((columnQualifier, value) -> {
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily2)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setTimestampMicros(System.currentTimeMillis() * 1000)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutationListBuilder.add(mutation);
        });
        ImmutableList<Mutation> mutations = mutationListBuilder.build();
        return KV.of(key, mutations);
    }

    private static KV<ByteString, Iterable<Mutation>> cbtKV(String keyName, List<String> cellTextList) {
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
                .setFamilyName(OPTIONAL_KEY_CF)
                .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                .setTimestampMicros(System.currentTimeMillis() * 1000)
                .setValue(ByteString.copyFromUtf8(text))
                .build();
    }



    private static void runBashProcess(String cmd) throws IOException, InterruptedException {
        LOGGER.info("executing {}", cmd);
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.command("bash", "-c", cmd);
        Process process = processBuilder.start();
        logProcess(process);
        int statusCode = process.waitFor();
        Assert.assertEquals(cmd + " failed", 0, statusCode);
    }

    private static void logProcess(Process process) throws IOException {
        try (BufferedReader rdr = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = rdr.readLine()) != null) {
                LOGGER.info(line);
            }
        }
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

// // cbt deletetable my-table
