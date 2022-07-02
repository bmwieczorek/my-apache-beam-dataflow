package com.bawi.beam.dataflow.other;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaStreamTest {
    public static void main(String[] args) {
        List<String> withNulls = Stream.of("a", "b", "c", "d").map(JavaStreamTest::toRandomUpperString).collect(Collectors.toList());
        System.out.println(withNulls); // [null, B, null, D]


        List<String> withoutNulls = Stream.of("a", "b", "c", "d")
                .map(JavaStreamTest::toRandomUpperString)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        System.out.println(withoutNulls); // [C, D]



        String[] nullableArray = new Random().nextBoolean() ?  new String[] { "a", "b"} : null;
        List<String> strings = Optional.ofNullable(nullableArray).map(Arrays::asList).orElse(Collections.emptyList());
        System.out.println(strings);

        // java 9
        //Stream<Integer> integerStream = Stream.of("a", "1", "b", "2").map(TestStream::toNumber).flatMap(Optional::stream);

        // java 8
        Stream<Integer> integerStream = Stream.of("a", "1", "b", "2").map(JavaStreamTest::toNumber).flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty());

        System.out.println(integerStream.collect(Collectors.toList()));
    }


    private static String toRandomUpperString(String s) {
        return new Random().nextBoolean() ? s.toUpperCase() : null;
    }

    private static Optional<Integer> toNumber(String s) {
        try {
            return Optional.of(Integer.parseInt(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
