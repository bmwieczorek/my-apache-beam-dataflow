package com.bawi.beam.dataflow;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableBiFunction;

public class CompositeValueProvider<T1, T2, R> implements ValueProvider<R> {
    private final ValueProvider<T1> valueProvider1;
    private final ValueProvider<T2> valueProvider2;
    private final SerializableBiFunction<T1, T2, R> translationBiFunction;

    private CompositeValueProvider(ValueProvider<T1> valueProvider1, ValueProvider<T2> valueProvider2,
                                  SerializableBiFunction<T1, T2, R> translationBiFunction) {
        this.valueProvider1 = valueProvider1;
        this.valueProvider2 = valueProvider2;
        this.translationBiFunction = translationBiFunction;
    }

    public static <T1, T2, R> CompositeValueProvider<T1, T2, R> of(ValueProvider<T1> valueProvider1, ValueProvider<T2> valueProvider2,
                                                                   SerializableBiFunction<T1, T2, R> translationBiFunction) {
        return new CompositeValueProvider<>(valueProvider1, valueProvider2, translationBiFunction);
    }

    @Override
    public R get() {
        return translationBiFunction.apply(valueProvider1.get(), valueProvider2.get());
    }

    @Override
    public boolean isAccessible() {
        return valueProvider1.isAccessible() && valueProvider2.isAccessible();
    }

    @Override
    public String toString() {
        if (isAccessible()) {
            return String.valueOf(get());
        }
        return this.getClass().getSimpleName() + "(valueProvider1=" + valueProvider1 + ", valueProvider2=" + valueProvider2
                + ", translationBiFunction=" + translationBiFunction + ")";
    }
}
