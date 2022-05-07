package com.bawi.beam.dataflow;

import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MyToListFn<T> extends Combine.CombineFn<T, MyToListFn.Accum<T>, List<T>> {
    public static class Accum<T> implements Serializable {
        List<T> objects = new ArrayList<>();

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Accum<?> accum = (Accum<?>) o;
            return Objects.equals(objects, accum.objects);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objects);
        }

    }

    public Accum<T> createAccumulator() {
        return new Accum<>();
    }

    public Accum<T> addInput(Accum<T> accum, T input) {
        accum.objects.add(input);
        return accum;
    }

    public Accum<T> mergeAccumulators(Iterable<Accum<T>> accums) {
        Accum<T> merged = createAccumulator();
        for (Accum<T> accum : accums) {
            merged.objects.addAll(accum.objects);
        }
        return merged;
    }

    public List<T> extractOutput(Accum<T> accum) {
        return accum.objects;
    }
}