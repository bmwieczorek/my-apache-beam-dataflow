package com.bawi.beam.dataflow;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

import static com.bawi.beam.dataflow.MyBQReadWriteJob.MySubscription.getBigDecimal;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;

public class MyBQReadWriteJobTest {

    @Test
    public void testBigQueryGenericRecordTypesConversion() {
        GenericRecord bqReadRecord = new GenericData.Record(MyBQReadWriteJob.MySubscription.SCHEMA);

        DateTimeFormatter timeFormatter = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

        DateTimeFormatter dateTimeFormatter =
                new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss") // .parseLenient()
                        .appendFraction(MICRO_OF_SECOND, 0, 6, true).toFormatter();

        DateTimeFormatter dateTimeWithTFormatter =
                new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss") // .parseLenient()
                        .appendFraction(MICRO_OF_SECOND, 0, 6, true).toFormatter();

//        "creation_timestamp": "2021-03-03 12:12:12.123",
//        long create_timestamp = System.currentTimeMillis() * 1000L;
        long create_timestamp = LocalDateTime.parse("2021-03-03 12:12:12.123", dateTimeFormatter)
                .atZone(UTC).toInstant().toEpochMilli() * 1000L;
        bqReadRecord.put("creation_timestamp", create_timestamp); // 1614769871000000

//        {"name": "expiration_date", "type": {"type": "int", "logicalType": "date"}}
//        "expiration_date": "2021-03-03",
        String expiration_date =  "2021-03-03";
        int epochDay = (int) LocalDate.parse(expiration_date).toEpochDay();
        Assert.assertEquals(18689, epochDay);
        bqReadRecord.put("expiration_date", epochDay); // 18689

//        {"name" : "my_time", "type" : {"type" : "long", "logicalType" : "time-micros"}}
//        my_time": "12:12:12.123"
        String my_time = "12:12:12.123000";
        LocalTime localTime = LocalTime.parse(my_time, timeFormatter);
        long epochTimeMicros = localTime.toNanoOfDay() / 1000;
        Assert.assertEquals( 43932123000L, epochTimeMicros);
        bqReadRecord.put("my_time", epochTimeMicros); // 43932123000

//        {"name" : "my_datetime", "type" : {"type" : "long", "logicalType" : "local-timestamp-micros"}}
//        "my_datetime": "2021-03-03 12:12:12.123"
        String my_datetime = "2021-03-03T12:12:12";
        LocalDateTime localDateTime = LocalDateTime.parse(my_datetime, dateTimeWithTFormatter);
        ZonedDateTime zonedDateTime = localDateTime.atZone(UTC);
        long epochDateTimeMicros = zonedDateTime.toInstant().toEpochMilli() * 1000;
        Assert.assertEquals(1614773532000000L, epochDateTimeMicros);
        bqReadRecord.put("my_datetime", new Utf8(my_datetime));  // 2021-03-03T12:12:12.123

//        {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9}
//        "my_numeric": 0.123456789,
        double d = 0.123456789;
        BigDecimal bigDecimal = BigDecimal.valueOf(d).setScale(9, RoundingMode.UNNECESSARY);
        BigInteger bigInteger = bigDecimal.unscaledValue();
        bqReadRecord.put("my_numeric", ByteBuffer.wrap(bigInteger.toByteArray())); // bytes e.g I\u008DX\u0080

//        {"name": "myRequiredSubRecord", "type": {"type": "record", "name": "myRequiredSubRecordType", "fields": [{"name": "myRequiredInt", "type": "int"}, {"name": "myNullableLong", "type": ["long", "null"]}, {"name": "myRequiredBoolean", "type": "boolean"}]}
//        "myRequiredSubRecord": { "myRequiredInt": 2, "myRequiredBoolean": true}
        GenericData.Record myRequiredSubRecord = new GenericData.Record(MyBQReadWriteJob.MySubscription.SCHEMA.getField("myRequiredSubRecord").schema());
        myRequiredSubRecord.put("myRequiredInt", 1L);
        myRequiredSubRecord.put("myRequiredBoolean", true);
        bqReadRecord.put("myRequiredSubRecord", myRequiredSubRecord);

        MyBQReadWriteJob.MySubscription mySubscription = MyBQReadWriteJob.MySubscription.fromGenericRecord(bqReadRecord);
        GenericRecord genericRecord = mySubscription.toGenericRecord();
        Assert.assertEquals(create_timestamp, genericRecord.get("creation_timestamp"));
        Assert.assertEquals(epochDay, genericRecord.get("expiration_date"));
        Assert.assertEquals(epochTimeMicros, genericRecord.get("my_time"));
        long alternativeEpochDateTimeMicros = LocalDateTime.parse("2024-08-28T12:34:12.567", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")).toInstant(UTC).toEpochMilli() * 1000;
        Assert.assertTrue(epochDateTimeMicros == (Long) genericRecord.get("my_datetime") || alternativeEpochDateTimeMicros == (Long) genericRecord.get("my_datetime"));

        BigDecimal myNumericActual = getBigDecimal(genericRecord, "my_numeric");
        BigDecimal alternativeBigDecimal = BigDecimal.valueOf(1.234d).setScale(9, RoundingMode.UNNECESSARY);
        Assert.assertTrue(bigDecimal.compareTo(myNumericActual) == 0 || alternativeBigDecimal.compareTo(myNumericActual) == 0);

        Object myRequiredSubRec = genericRecord.get("myRequiredSubRecord");
        Assert.assertEquals(1, ((GenericRecord) myRequiredSubRec).get("myRequiredInt"));
    }
}
