package com.bawi.beam.dataflow;

import com.google.api.gax.rpc.ServerStream;
import com.google.bigtable.v2.*;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowCell;
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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

public class MyBigtableWriteReadTest {
    private static final Instant NOW = Instant.now();
    public static final Instant NOW_PLUS_3_SECS = NOW.plusMillis(3000);


    @BeforeClass
    public static void setup() throws IOException, InterruptedException {
        // cbt createinstance my-instance my-instance-name my-cluster us-central1-a 1 HDD
        // echo project = `gcloud config get-value project` > ~/.cbtrc
        // echo instance = my-instance >> ~/.cbtrc
        // cbt createtable my-table
        runBashProcess("cbt createfamily my-table " + OPTIONAL_KEY_CF);
        runBashProcess("cbt createfamily my-table " + VALUE_CF);

        Pipeline writePipeline = TestPipeline.create(OPTIONS);
        PCollection<KV<ByteString, Iterable<Mutation>>> input =
                writePipeline.apply(Create.of(
//                        RQ_KEY                           OPTIONAL_KEY_CF:RQ_CODE,RQ_SERVICE,RQ_TS  VALUE_CF:ID,SUB_ID,
//                        aacc#9223370366594304660#AcSG    optional_key_cf:A1C1,SERV2                value_cf:ac1,1

                        // same key prefix, different last portion (revert timestamp newer on the top)
                        cbtKV("aabb#" + reverseTs(NOW) + "#" + randomString(), NOW,
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "ab1", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + reverseTs(NOW.plusSeconds(2)) + "#" + randomString(), NOW.plusSeconds(2),
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "ab2", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + reverseTs(NOW.plusSeconds(1)) + "#" + randomString(), NOW.plusSeconds(1),
                                OPTIONAL_KEY_CF, Collections.emptyMap(),
                                VALUE_CF,      Map.of(ID_CQ, "ab3", SUB_ID_CQ, "1")),

                        // by rq_id (unique)
                        cbtKV("aabb#" + reverseTs(NOW_PLUS_3_SECS) + "#" + randomString(), NOW_PLUS_3_SECS,
                                OPTIONAL_KEY_CF, Map.of(RQ_ID_CQ, "abc123", CODE_CQ, "A1B1_"),
                                VALUE_CF,      Map.of(ID_CQ, "ab4", SUB_ID_CQ, "1")),

                        // most recent by code and service
                        cbtKV("aabb#" + reverseTs(NOW.plusSeconds(4)) + "#" + randomString(), NOW.plusSeconds(4),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1B1", SERVICE_CQ, "Serv1"),
                                VALUE_CF,      Map.of(ID_CQ, "ab5", SUB_ID_CQ, "1")),

                        cbtKV("aabb#" + reverseTs(NOW.plusSeconds(5)) + "#" + randomString(), NOW.plusSeconds(5),
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1B1_", SERVICE_CQ, "Serv1"),
                                VALUE_CF,      Map.of(ID_CQ, "ab6", SUB_ID_CQ, "1")),

                        // single row with aacc row prefix
                        cbtKV("aacc#" + reverseTs(NOW) + "#" + randomString(), NOW,
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1C1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ac1", SUB_ID_CQ, "1")),

                        // two rows with exactly the same key (api stores only one of them)
                        cbtKV("aadd#" + reverseTs(NOW) + "#AbCd", NOW,
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1D1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ad1", SUB_ID_CQ, "1")),

                        cbtKV("aadd#" + reverseTs(NOW) + "#AbCd", NOW,
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1D1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ad2", SUB_ID_CQ, "2"))

                ).withCoder(getCoder()));
        input.apply(BigtableIO.write().withProjectId(PROJECT).withInstanceId(OPTIONS.getInstanceId()).withTableId(OPTIONS.getTableId()));
        writePipeline.run().waitUntilFinish();

        Pipeline writePipeline2 = TestPipeline.create(OPTIONS);
        PCollection<KV<ByteString, Iterable<Mutation>>> input2 =
                writePipeline2.apply(Create.of(
                        // additional row with the same key as above but inserted by other pipeline
                        cbtKV("aadd#" + reverseTs(NOW) + "#AbCd", NOW,
                                OPTIONAL_KEY_CF, Map.of(CODE_CQ, "A1D1", SERVICE_CQ, "Serv2"),
                                VALUE_CF,      Map.of(ID_CQ, "ad3", SUB_ID_CQ, "3"))

                ).withCoder(getCoder()));
        input2.apply(BigtableIO.write().withProjectId(PROJECT).withInstanceId(OPTIONS.getInstanceId()).withTableId(OPTIONS.getTableId()));
        writePipeline2.run().waitUntilFinish();

        runBashProcess("cbt read my-table");
    }

    private static long reverseTs(Instant now) {
        return Long.MAX_VALUE - now.toEpochMilli();
    }

    @Test
    public void usingBigTableClientInProcessShouldFilter() {
        Pipeline readPipeline = TestPipeline.create(OPTIONS);

        PCollection<String> read = readPipeline
                .apply("Key prefix", Create.of("aa")) // here key prefix includes all rows
                .apply(ParDo.of(new ReadRowsFn()))
                .apply(MapElements.into(TypeDescriptors.strings()).via(row -> {
                    String key = row.getKey().toStringUtf8();
                    String keyPrefix = key.substring(0, key.indexOf("#") + 1);
                    List<RowCell> cells = row.getCells();
                    String result = keyPrefix + "... " + cells.stream().map(cell -> {
                        String family = cell.getFamily();
                        String qualifier = cell.getQualifier().toStringUtf8();
                        String value = cell.getValue().toStringUtf8();
                        List<String> labels = cell.getLabels();
//                        LOGGER.info("by key: key={},family={},qualifier={},column={},labels={}", key, family, qualifier, value, labels);
                        return family + ":" + qualifier + "=" + value + "->" + labels;
                    }).collect(Collectors.joining(","));
                    LOGGER.info("Read filtered: {}", result);
                    return result;
                }));

//        filtered out aacc and aadd row as not matching timestamp range for aabb
//        PAssert.that(read).containsInAnyOrder(List.of(
//                "aabb#... optional_key_cf:code_cq=A1B1_->[e-rq-id--r-code],optional_key_cf:rq_id_cq=abc123->[e-rq-id--r-code],value_cf:id_cq=ab4->[e-rq-id--r-code],value_cf:sub_id_cq=1->[e-rq-id--r-code]",
//                "aacc#... optional_key_cf:code_cq=A1C1->[e-code],optional_key_cf:service_cq=Serv2->[e-code],value_cf:id_cq=ac1->[e-code],value_cf:sub_id_cq=1->[e-code]",
//                "aadd#... optional_key_cf:code_cq=A1D1->[e-code],optional_key_cf:service_cq=Serv2->[e-code],value_cf:id_cq=ad3->[e-code],value_cf:sub_id_cq=3->[e-code]"
//        ));
        PAssert.thatSingleton(read).isEqualTo(
                "aabb#... optional_key_cf:code_cq=A1B1_->[e-rq-id--r-code],optional_key_cf:rq_id_cq=abc123->[e-rq-id--r-code],value_cf:id_cq=ab4->[e-rq-id--r-code],value_cf:sub_id_cq=1->[e-rq-id--r-code]");

        readPipeline.run().waitUntilFinish();
    }

    private static class ReadRowsFn extends DoFn<String, com.google.cloud.bigtable.data.v2.models.Row> {
        private BigtableDataClient dataClient;

        @Setup
        public void setup() throws IOException {
            LOGGER.info("Creating BigtableDataClient");
            dataClient = BigtableDataClient.create(PROJECT, INSTANCE_ID);
        }

        @ProcessElement
        public void process(@Element String key, OutputReceiver<com.google.cloud.bigtable.data.v2.models.Row> outputReceiver) {
            ServerStream<com.google.cloud.bigtable.data.v2.models.Row> rows
                    = dataClient.readRows(Query.create(TABLE_ID)
                    .range(key, key + "~") // filter
                    .filter(FILTERS.chain()
                            .filter(FILTERS.limit().cellsPerColumn(1)) // include only the newest cell values
                            .filter(FILTERS.timestamp().range() // filter out rows with cells outside timestamp range (other than aabb# id ab4)
                                    .startClosed(NOW_PLUS_3_SECS.minusMillis(10).toEpochMilli() * 1000)
                                    .endOpen(NOW_PLUS_3_SECS.plusMillis(10).toEpochMilli() * 1000))
                            .filter(FILTERS
                                    .condition(FILTERS.chain().filter(FILTERS.qualifier().exactMatch(RQ_ID_CQ)).filter(FILTERS.value().exactMatch("abc123")))
                                    .then(
                                            FILTERS.condition(FILTERS.chain()
                                                            .filter(FILTERS.pass())
                                                            .filter(FILTERS.chain().filter(FILTERS.qualifier().exactMatch(CODE_CQ)).filter(FILTERS.value().regex(".*_$")))
                                                    )
                                                    // return entire row and label it e-rq-id--r-code if rq_id = abc123 and if code ends with '_'
                                                    .then(FILTERS.label("e-rq-id--r-code"))
                                                    .otherwise(FILTERS.block())
                                    )
                                    .otherwise(
                                            FILTERS.condition(FILTERS.chain().filter(FILTERS.qualifier().exactMatch(SERVICE_CQ)).filter(FILTERS.value().exactMatch("Serv2")))
                                                    // return entire row and label "e-code" if service = Serv2
                                                    .then(FILTERS.label("e-code"))
                                                    .otherwise(FILTERS.block())
                                    )
                            )
                    )
            );
            rows.forEach(outputReceiver::output);
        }

        @Teardown
        public void teardown() {
            dataClient.close();
            LOGGER.info("Closed BigtableDataClient");
        }

    }

    @Test
    public void readEntireRowFilteringByKeyAndValueCondition() {
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

        PCollection<String> read = readPipeline
                .apply(BigtableIO.read()
                        .withProjectId(PROJECT)
                        .withInstanceId(OPTIONS.getInstanceId())
                        .withTableId(OPTIONS.getTableId())
//                        .withKeyRange(ByteKeyRange.of(ByteKey.copyFrom("aabb#".getBytes()), ByteKey.copyFrom("aabb#~".getBytes()))) // returns 7 rows out of 10 rows
                        .withKeyRange(ByteKeyRange.ALL_KEYS.withStartKey(ByteKey.copyFrom("aabb#".getBytes()))) // (same as above) all keys starting with aabb# (inc) onwards
                        .withRowFilter(RowFilter.newBuilder().setCondition(condition).build()) // filter only one row with rq_id=abc123
                )
                .apply(MapElements.into(TypeDescriptors.strings()).via(MyBigtableWriteReadTest::toString));

        PAssert.that(read).satisfies((Iterable<String> stringIterable) -> {
            LOGGER.info("Read filtered: {}", stringIterable);
            List<String> strings = StreamSupport.stream(stringIterable.spliterator(), false).collect(Collectors.toList());
            Assert.assertEquals(1, strings.size());
            Assert.assertEquals("aabb#... optional_key_cf:code_cq=A1B1_,rq_id_cq=abc123|value_cf:id_cq=ab4,sub_id_cq=1", strings.get(0));
            return null;
        });
        readPipeline.run().waitUntilFinish();
    }

    private static String toString(Row row) {
        String key = row.getKey().toStringUtf8();
        String keyPrefix = key.substring(0, key.indexOf("#") + 1);
        List<Family> families = row.getFamiliesList();
        String familyColumnCellValues = families.stream().map((Family family) -> {
            List<Column> columns = family.getColumnsList();
            return family.getName() + ":" + columns.stream().map((Column column) -> {
                List<Cell> cells = column.getCellsList();
                return column.getQualifier().toStringUtf8() + "=" + cells.stream().map((Cell cell) -> {
//                                LOGGER.info("by rq_iq:  key={},family={},column={},cell={}", key, family.getName(), column.getQualifier().toStringUtf8(), cell);
                    return cell.getValue().toStringUtf8();
                }).collect(Collectors.joining(";"));
            }).collect(Collectors.joining(","));
        }).collect(Collectors.joining("|"));
        return keyPrefix + "... " + familyColumnCellValues;
    }

    @Test
    public void shouldFilterRowAndReadPartially() {
        Pipeline readPipeline = TestPipeline.create(OPTIONS);

        PCollection<String> read = readPipeline
                .apply(BigtableIO.read()
                                .withProjectId(PROJECT)
                                .withInstanceId(OPTIONS.getInstanceId())
                                .withTableId(OPTIONS.getTableId())
                                .withKeyRange(ByteKeyRange.ALL_KEYS.withStartKey(ByteKey.copyFrom("aa#".getBytes())))

                                // chained filter (all applied)
                                .withRowFilter(RowFilter.newBuilder().setChain(
                                        RowFilter.Chain.newBuilder().addAllFilters(
                                                List.of(
                                                        RowFilter.newBuilder().setFamilyNameRegexFilter(OPTIONAL_KEY_CF + ".*").build(),
                                                        RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build() // latest version based on timestamp
//                                    RowFilter.newBuilder().setValueRegexFilter(ByteString.copyFromUtf8(".*1$")).build()
                                                )
                                        )
                                ).build())
                )
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via(row -> {
                    AtomicBoolean rq_id_found = new AtomicBoolean(false);
                    AtomicBoolean code_ext_found = new AtomicBoolean(false);
                    List<Family> families = row.getFamiliesList();
                    families.forEach(family -> {
                        List<Column> columns = family.getColumnsList();
                        columns.forEach((Column column) -> {
                            List<Cell> cells = column.getCellsList();
                            cells.forEach(cell -> {
                                String columnName = column.getQualifier().toStringUtf8();
                                String cellValue = cell.getValue().toStringUtf8();
                                if (OPTIONAL_KEY_CF.equals(family.getName())) {
                                    if (RQ_ID_CQ.equals(columnName) && ("abc123".equals(cellValue))) {
                                        rq_id_found.set(true);
                                    }
                                    if (CODE_CQ.equals(columnName) && cellValue.endsWith("_")) {
                                        code_ext_found.set(true);
                                    }
                                }
                            });
                        });
                    });
                    if (rq_id_found.get() && code_ext_found.get()) {
                        LOGGER.info("found row={}", toString(row));
                        return List.of(toString(row));
                    }
                    return Collections.emptyList();
                }));

        PAssert.thatSingleton(read).isEqualTo("aabb#... optional_key_cf:code_cq=A1B1_,rq_id_cq=abc123");
        readPipeline.run().waitUntilFinish();
    }

    @AfterClass
    public static void after() throws IOException, InterruptedException {
        runBashProcess("cbt deletefamily my-table " + OPTIONAL_KEY_CF);
        runBashProcess("cbt deletefamily my-table " + VALUE_CF);
    }

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String INSTANCE_ID = "my-instance";
    private static final String TABLE_ID = "my-table";

    private static final  MyBigtablePipelineOptions OPTIONS = createPipelineOptions(PROJECT);

    private static final Logger LOGGER = LoggerFactory.getLogger(MyBigtableWriteReadTest.class);
    public static final String OPTIONAL_KEY_CF = "optional_key_cf";
    public static final String RQ_ID_CQ = "rq_id_cq";
    public static final String CODE_CQ = "code_cq";
    public static final String SERVICE_CQ = "service_cq";

    public static final String VALUE_CF = "value_cf";
    public static final String ID_CQ = "id_cq";
    public static final String SUB_ID_CQ = "sub_id_cq";


    @SuppressWarnings("unused")
    public interface MyBigtablePipelineOptions extends PipelineOptions {
        @Default.String(INSTANCE_ID)
        @Validation.Required
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> value);

        @Default.String(TABLE_ID)
        @Validation.Required
        ValueProvider<String> getTableId();
        void setTableId(ValueProvider<String> value);

    }

    @SuppressWarnings("SameParameterValue")
    private static MyBigtablePipelineOptions createPipelineOptions(String project) {
        String[] args = {"--project=" + project, "--tableId=my-table"};
//        args = DataflowUtils.updateDataflowArgs(args);
        return PipelineOptionsFactory.fromArgs(args).as(MyBigtablePipelineOptions.class);
    }

    private static String randomString() {
        return RandomStringUtils.randomAlphanumeric(4);
    }


    private static KvCoder<ByteString, Iterable<Mutation>> getCoder() {
        return KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)));
    }


    @SuppressWarnings("SameParameterValue")
    private static KV<ByteString, Iterable<Mutation>> cbtKV(String rowKey, Instant instant,
                                                            String columnFamily1, Map<String, String> columnQualifierAndValueList1,
                                                            String columnFamily2, Map<String, String> columnQualifierAndValueList2) {
        ByteString key = ByteString.copyFromUtf8(rowKey);
        ImmutableList.Builder<Mutation> mutationListBuilder = ImmutableList.builder();
        columnQualifierAndValueList1.forEach((columnQualifier, value) -> {
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily1)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setTimestampMicros(instant.toEpochMilli() * 1000)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutationListBuilder.add(mutation);
        });
        columnQualifierAndValueList2.forEach((columnQualifier, value) -> {
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                    .setFamilyName(columnFamily2)
                    .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
                    .setTimestampMicros(instant.toEpochMilli() * 1000)
                    .setValue(ByteString.copyFromUtf8(value))
                    .build();
            Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
            mutationListBuilder.add(mutation);
        });
        ImmutableList<Mutation> mutations = mutationListBuilder.build();
        return KV.of(key, mutations);
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
// cbt set my-table myKey1 my-column-family1:my-column-qualifier1=abc1
// cbt set my-table myKey1 my-column-family1:my-column-qualifier1=abc11
// cbt set my-table myKey1 my-column-family1:my-column-qualifier2=def1
// cbt set my-table myKey1 my-column-family1:my-column-qualifier2=def11
// cbt read my-table
//myKey1
//        my-column-family1:my-column-qualifier1   @ 2022/11/23-14:32:20.953000
//        "abc11"
//        my-column-family1:my-column-qualifier1   @ 2022/11/23-14:22:37.980000
//        "abc1"
//        my-column-family1:my-column-qualifier2   @ 2022/11/23-14:32:20.953000
//        "def11"
//        my-column-family1:my-column-qualifier2   @ 2022/11/23-14:22:37.980000
//        "def1"

// // cbt deletetable my-table
