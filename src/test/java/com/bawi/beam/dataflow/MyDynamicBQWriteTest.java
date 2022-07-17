package com.bawi.beam.dataflow;

import com.bawi.beam.dataflow.bigquery.TableRowWithSchema;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyDynamicBQWriteTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testE2E() {
        PCollection<TableRowWithSchema> pCollection = pipeline.apply(new MyDynamicBQWriteJob.MyDynamicBQWritePTransform());

        PAssert.that(pCollection).satisfies(new MatchesFunction(
                Map.of("optional_record",
                        Set.of("{\"myOptionalString\":\"abc\"}"),
                        "required_record",
                        Set.of("{" +
                                "\"myRequiredString\":\"abc\"," +
                                "\"myRequiredInt\":\"123\"," +
                                "\"myRequiredDate\":\"2017-01-01\"," +
                                "\"myRequiredTimestamp\":\"2022-03-20 03:41:42.123000 UTC\"," +
                                "\"myRequiredBoolean\":true," +
                                "\"myRequiredNumeric\":\"1.230000000\"," +
                                "\"myRequiredDouble\":4.56," +
                                "\"myRequiredTime\":\"12:34:56.789\"," +
                                "\"myRequiredBytes\":\"YWJjMTIz\"," +
                                "\"myRequiredFloat\":7.89," +
                                "\"myRequiredLong\":\"567\"" +
                                "}"
                        ))));

        pipeline.run().waitUntilFinish();
    }

    private static class MatchesFunction implements SerializableFunction<Iterable<TableRowWithSchema>, Void> {

        private final Map<String, Set<String>> expected;
        private Map<String, Set<String>> actual = new HashMap<>();
        private MatchesFunction(Map<String, Set<String>> expected) {
            this.expected = expected;
        }

        @Override
        public Void apply(Iterable<TableRowWithSchema> input) {
            input.forEach(tRowSchema -> {
                TableRow tableRow = tRowSchema.getTableRow();
                tableRow.setFactory(new GsonFactory());
                actual.merge(tRowSchema.getDataType(), Set.of(tableRow.toString()), (Set<String> prevSet, Set<String> currSet) -> {
                    prevSet.addAll(currSet);
                    return prevSet;
                });
            });
            Assert.assertEquals(expected, actual);
            return null;
        }
    }
}
