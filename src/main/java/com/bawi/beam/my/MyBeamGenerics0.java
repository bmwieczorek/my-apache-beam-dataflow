package com.bawi.beam.my;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MyBeamGenerics0 {
    static class MyPCollection<T> {
        private List<T> elements;

        public MyPCollection(List<T> elements) {
            this.elements = elements;
        }

        public <Out> MyPCollection<Out> apply(MyPTransform2<T, Out> myPTransform2) {
            return myPTransform2.transform(this);
        }

        @Override
        public String toString() {
            return "MyPCollection{elements=" + elements + '}';
        }
    }

    static class MyPTransform3<In extends MyPCollection<In>, Out extends MyPCollection<Out>> extends MyPTransform<MyPCollection<In>, MyPCollection<Out>> {
        Out transform(In in) {
            return null;
        }
    }

    static class MyPTransform2<In, Out> extends MyPTransform<MyPCollection<In>, MyPCollection<Out>> {
    }


    static abstract class MyPTransform<In, Out> {
        Out transform(In in) {
            return null;
        }
    }

    static class MyCreate {
        static <T> MyPTransform2<Void, T> of(T t, T... tt) {
            List<T> list = new ArrayList<>();
            list.add(t);
            list.addAll(Arrays.asList(tt));
            return new MyPTransform2<>() {
                @Override
                MyPCollection<T> transform(MyPCollection<Void> voidMyPCollection) {
                    return new MyPCollection<>(list);
                }
            };
        }
    }

    @FunctionalInterface
    interface FuncMapFn<In, Out> {
        Out map(In in);
    }

    static abstract class MapFn<In, Out> {
        abstract Out map(In in);
    }

    static class MyMapElements<In, Out> extends MyPTransform2<In, Out> {
        <NewIn> MyMapElements<NewIn, Out> via(FuncMapFn<NewIn, Out> mapFn) {
            return new MyMapElements<>() {
                @Override
                MyPCollection<Out> transform(MyPCollection<NewIn> inMyPCollection) {
                    List<NewIn> inElements = inMyPCollection.elements;
                    List<Out> outElements = inElements.stream().map(mapFn::map).collect(Collectors.toList());
                    return new MyPCollection<>(outElements);
                }
            };
        }

        static <In, Out> MyMapElements<In, Out> map(MapFn<In, Out> mapFn) {
            return new MyMapElements<>() {
                @Override
                MyPCollection<Out> transform(MyPCollection<In> inMyPCollection) {
                    List<In> inElements = inMyPCollection.elements;
                    List<Out> outElements = inElements.stream().map(mapFn::map).collect(Collectors.toList());
                    return new MyPCollection<>(outElements);
                }
            };
        }

        static <Out> MyMapElements<?, Out> into(Class<Out> clazz) {
            return new MyMapElements<>();
        }
    }

    static class MyPipeline {
        <In, Out> MyPCollection<Out> apply(MyPTransform2<In, Out> myPTransform) {
            return myPTransform.transform(new MyPCollection<>(null));
        }
    }

    public static void main(String[] args) {
        MyPipeline myPipeline = new MyPipeline();
        MyPCollection<String> pStrings = myPipeline.apply(MyCreate.of("1", "2", "3"));
        System.out.println(pStrings);

        MyPCollection<Integer> pIntegers = pStrings.apply(MyMapElements.map(new MapFn<String, Integer>() {
            @Override
            Integer map(String s) {
                return Integer.parseInt(s);
            }
        }));
        System.out.println(pIntegers);

        MyPCollection<BigDecimal> pBigInteger = pIntegers.apply(MyMapElements.into(BigDecimal.class).via(MyBeamGenerics0::intToBigInteger));
        System.out.println(pBigInteger);
    }

    static BigDecimal intToBigInteger(Integer i) {
        BigDecimal bigDecimal = BigDecimal.valueOf(i);
        return bigDecimal.setScale(2, RoundingMode.HALF_EVEN);
    }
}
