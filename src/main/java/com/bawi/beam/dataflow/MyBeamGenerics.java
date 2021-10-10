package com.bawi.beam.dataflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyBeamGenerics {
    static class MyPCollection<T> {
        private List<T> elements;
        private Class<T> coderClass;

        public MyPCollection(List<T> elements, Class<T> coderClass) {
            this.elements = elements;
            this.coderClass = coderClass;
        }

        public <Out> MyPCollection<Out> apply(MyPTransform<MyPCollection<T>, MyPCollection<Out>> myPTransform) {
//            Class<Out> clazz = ((MyMapElements<T, Out>) myPTransform).getClazz();
//            System.out.println("Converting to " + clazz);
            return myPTransform.transform(this);
        }

        @Override
        public String toString() {
            return "MyPCollection{elements=" + elements + ", coderClass=" + coderClass + '}';
        }
    }

    static class MyPTransform<In, Out>  {
        Out transform(In in) {
            return null;
        }
    }

    static class MyCreate {
        static <T> MyPTransform<MyPCollection<Void>, MyPCollection<T>> of(T t, T... tt) {
//            System.out.println("Creating " + t.getClass());
            List<T> list = new ArrayList<>();
            list.add(t);
            list.addAll(Arrays.asList(tt));
            return new MyPTransform<>() {
                @Override
                MyPCollection<T> transform(MyPCollection<Void> voidMyPCollection) {
                    Class<T> aClass = (Class<T>) t.getClass();
                    return new MyPCollection<>(list, aClass);
                }
            };
        }
    }

    @FunctionalInterface
    interface FuncMapFn<In, Out> {
        Out map(In in);
    }

    static class MapFn<In, Out> {
        Out map(In in) {
            return null;
        }
    }


    static class MyParDo<In, Out> extends MyPTransform<MyPCollection<In>, MyPCollection<Out>> {
        private MyOutputReceiver<Out> myOutputReceiver = new MyOutputReceiver<>();
        private MyDoFn<In, Out> myDoFn;
        private MyParDo(MyDoFn<In, Out> myDoFn) {
            this.myDoFn = myDoFn;
        }

        @Override
        MyPCollection<Out> transform(MyPCollection<In> inMyPCollection) {
            Method method = Stream.of(myDoFn.getClass().getMethods()).filter(m -> "process".equals(m.getName())).findFirst().orElseThrow();
            inMyPCollection.elements.forEach(
                    element -> {
                        try {
                            method.invoke(myDoFn, element, myOutputReceiver);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            ex.printStackTrace();
                        }
                    }
            );
            Type actualTypeArgument = ((ParameterizedType) ((MyDoFn<In, Out>) myDoFn).getClass().getGenericSuperclass()).getActualTypeArguments()[1];
            return new MyPCollection<>(myOutputReceiver.getElements(), (Class<Out>)actualTypeArgument);
        }

        static <In, Out> MyParDo<In, Out> of(MyDoFn<In, Out> myDoFn){
            return new MyParDo<>(myDoFn);
        }
    }
    static class MyOutputReceiver<Out> {
        private List<Out> elements = new ArrayList<>();
        public List<Out> getElements() {
            return elements;
        }
        public void output(Out out) {
            elements.add(out);
        }
    }
    static abstract class MyDoFn<In, Out> {}

    static class MyStringToIntegerDoFn extends MyDoFn<String, Integer> {
        public void process(String element, MyOutputReceiver<Integer> myOutputReceiver) {
            int i = Integer.parseInt(element);
            myOutputReceiver.output(i);
        }
    }

    static class MyMapElements<In, Out> extends MyPTransform<MyPCollection<In>, MyPCollection<Out>> {
        private Class<Out> outClass;

        public MyMapElements(Class<Out> outClass) {
            this.outClass = outClass;
        }

        public Class<Out> getOutClazz() {
            return outClass;
        }

        <NewIn> MyMapElements<NewIn,Out> via(FuncMapFn<NewIn, Out> mapFn) {
            return new MyMapElements<>(this.getOutClazz()) {
                @Override
                MyPCollection<Out> transform(MyPCollection<NewIn> inMyPCollection) {
                    List<NewIn> inElements = inMyPCollection.elements;
                    List<Out> outElements = inElements.stream().map(mapFn::map).collect(Collectors.toList());
                    return new MyPCollection<>(outElements, this.getOutClazz());
                }
            };
        }

        static <In, Out> MyMapElements<In, Out> map(MapFn<In, Out> mapFn) {
            return new MyMapElements<>(null) {

                @Override
                MyPCollection<Out> transform(MyPCollection<In> inMyPCollection) {
                    Type[] actualTypeArguments = ((ParameterizedType) (((MapFn<In, Out>) mapFn).getClass()).getGenericSuperclass()).getActualTypeArguments();
//                    System.out.println("Converting " + actualTypeArguments[0] + " to " + actualTypeArguments[1]);
                    List<In> inElements = inMyPCollection.elements;
                    List<Out> outElements = inElements.stream().map(mapFn::map).collect(Collectors.toList());
                    return new MyPCollection<>(outElements, (Class<Out>) actualTypeArguments[1]);
                }
            };
        }

        static <Out> MyMapElements<?, Out> into(Class<Out> clazz) {
            return new MyMapElements<>(clazz);
        }
    }

    static class MyPipeline {
        <Out> MyPCollection<Out> apply(MyPTransform<MyPCollection<Void>, MyPCollection<Out>> myPTransform) {
            return myPTransform.transform(null);
        }
    }

    public static void main(String[] args) {
        MyPipeline myPipeline = new MyPipeline();
        MyPCollection<String> pStrings = myPipeline.apply(MyCreate.of("1", "2", "3"));
        System.out.println(pStrings);

        MyPCollection<Integer> pIntegers = pStrings.apply(MyParDo.of(new MyStringToIntegerDoFn()));
        System.out.println(pIntegers);

        MyPCollection<Double> pDoubles = pIntegers.apply(MyMapElements.map(new MapFn<Integer, Double>() {
                    @Override
                    Double map(Integer i) {
                        return (double) i / 2;
                    }
        }));
        System.out.println(pDoubles);

        MyPCollection<BigDecimal> pBigInteger = pDoubles.apply(MyMapElements.into(BigDecimal.class).via(MyBeamGenerics::intToBigInteger));
        System.out.println(pBigInteger);

        MyPCollection<BigDecimal> sum = pBigInteger.apply(new MyPTransform<>() {
            @Override
            MyPCollection<BigDecimal> transform(MyPCollection<BigDecimal> bigDecimalMyPCollection) {
                Optional<BigDecimal> reduce = bigDecimalMyPCollection.elements.stream().reduce(BigDecimal::add);
                return new MyPCollection<>(reduce.map(Collections::singletonList).orElseGet(ArrayList::new), BigDecimal.class);
            }
        });
        System.out.println(sum);
    }

    static BigDecimal intToBigInteger(Double i) {
        BigDecimal bigDecimal = BigDecimal.valueOf(i);
        return bigDecimal.setScale(2, RoundingMode.HALF_EVEN);
    }
}
