package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

import java.util.List;

public class MyInnerJoinTest {
    @Test
    public void testRowInnerJoin() {
        // given
        Schema nameSchema = Schema.builder().addInt32Field("id").addStringField("name").build();
        Row bobName = Row.withSchema(nameSchema).addValues(123, "bob").build();
        Row aliceName = Row.withSchema(nameSchema).addValues(567, "alice").build();

        Schema ageSchema = Schema.builder().addInt32Field("id").addInt32Field("age").build();
        Row bobAge = Row.withSchema(ageSchema).addValues(123, 10).build();
        Row aliceAge = Row.withSchema(ageSchema).addValues(567, 7).build();

        Pipeline pipeline = Pipeline.create();
        PCollection<Row> names = pipeline.apply("Create names",  Create.of(bobName, aliceName).withCoder(RowCoder.of(nameSchema)));
        PCollection<Row> ages = pipeline.apply("Create ages", Create.of(bobAge, aliceAge).withCoder(RowCoder.of(ageSchema)));

        // when
        PCollection<Row> joined = names.apply(Join.<Row, Row>innerJoin(ages).using("id"));

        // then
        PCollection<String> result = joined.apply(MapElements.into(TypeDescriptors.strings()).via((Row row) -> {
            Row nameR = row.getRow(0);
            Row ageR = row.getRow(1);
            String name = nameR == null ? null : nameR.getString("name");
            Integer age = ageR == null ? null : ageR.getInt32("age");
            return name + "," + age;
        }));

        PAssert.that(result).containsInAnyOrder(List.of("bob,10", "alice,7"));

        pipeline.run().waitUntilFinish();
    }
}
