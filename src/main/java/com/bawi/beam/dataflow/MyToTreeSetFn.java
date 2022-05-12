package com.bawi.beam.dataflow;

import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.Objects;
import java.util.TreeSet;

public class MyToTreeSetFn<T extends Comparable<T>> extends Combine.CombineFn<T, MyToTreeSetFn.Accum<T>, TreeSet<T>> {

    public static class Accum<T extends Comparable<T>> implements Serializable {
        private TreeSet<T> objects = new TreeSet<>();

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

    @Override
    public Accum<T> createAccumulator() {
        return new Accum<>();
    }

    @Override
    public Accum<T> addInput(Accum<T> accum, T input) {
        accum.objects.add(input);
        return accum;
    }

    @Override
    public Accum<T> mergeAccumulators(Iterable<Accum<T>> accums) {
        Accum<T> merged = createAccumulator();
        for (Accum<T> accum : accums) {
            merged.objects.addAll(accum.objects);
        }
        return merged;
    }

    @Override
    public TreeSet<T> extractOutput(Accum<T> accum) {
        return accum.objects;
    }
}