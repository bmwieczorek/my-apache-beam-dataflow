package com.bawi.beam.dataflow;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyConsoleIO<T> extends PTransform<PCollection<T>, PDone> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyConsoleIO.class);

    private MyConsoleIO() { }

    public static <T> MyConsoleIO<T> write() {
        return new MyConsoleIO<>();
    }

    @Override
    public PDone expand(PCollection<T> input) {
        input.apply("MyConsoleIO", MapElements.via(new SimpleFunction<T, Void>() {
            @Override
            public Void apply(T input) {
                LOGGER.info("ConsoleIO LOGGER: {}", input);
                System.out.println("ConsoleIO System.out: " + input);
                return null;
            }
        }));
        return PDone.in(input.getPipeline());
    }
}
