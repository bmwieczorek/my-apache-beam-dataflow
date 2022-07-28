package com.bawi.beam.my;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class MyParallelCollection {

    public static void main(String[] args) {
        MyJavaDataset<String> stringMyJavaDataset = MyJavaDataset.create(Arrays.asList("a", "bb", "ccc"));

//        MyJavaDataset<String> sssStringMyJavaDataset = stringMyJavaDataset.map(new Function<String, String>() {
//            @Override
//            public String apply(String s) {
//                return s;
//            }
//        });

// or lambda
//        MyJavaDataset<String> sssStringMyJavaDataset = stringMyJavaDataset.map(s -> (s + s));

//        MyJavaDataset<String> sssStringMyJavaDataset2 = sssStringMyJavaDataset.map(new MyAbstractFunction<String, String>() {
//            @Override
//            public String apply(String s) {
//                return s + s;
//            }
//        });

//        MyJavaDataset<Integer> integerMyJavaDataset = sssStringMyJavaDataset2.map(s -> s.length());

// or lambda
//        MyJavaDataset<Integer> integerMyJavaDataset = sssStringMyJavaDataset.map(String::length);

        MapElements<String, Integer> fn = MapElements
                .onto(Integer.class)
                .via(String::length);

        MyJavaDataset<Integer> integerMyJavaDataset = stringMyJavaDataset.map(fn);
//        MyJavaDataset<Integer> integerMyJavaDataset = stringMyJavaDataset.map(MapElements.via(new MyAbstractFunction<String, Integer>() {
//
//            @Override
//            public Integer apply(String element) {
//                return element.length();
//            }
//        }));

        integerMyJavaDataset.show();
    }


    public static abstract class MyAbstractFunction<T, R> implements Function<T, R> {
        public abstract R apply(T element);
    }

    public static class MapElements <T, R> extends MyAbstractFunction<T, R> {
        private final Class<R> clazz;
        private final Function<T, R> function;

        public MapElements(Class<R> clazz, Function<T, R> function) {
            this.clazz = clazz;
            this.function = function;
        }

        public static <R> MapElements<?, R> onto(Class<R> clazz) {
            return new MapElements<>(clazz, null);
        }

        public <NewT> MapElements<NewT, R> via(Function<NewT, R> function) {
            return new MapElements<>(clazz, function);
        }

        public static <T, R> MapElements<T, R> via(MyAbstractFunction<T, R> myAbstractFunction) {
            Class<R> outputType = getOutputType(myAbstractFunction);
            return new MapElements<>(outputType, myAbstractFunction);
        }

        @Override
        public R apply(T element) {
            return function.apply(element);
        }

        public Class<R> getClazz() {
            return clazz;
        }
    }

    static  Map<Class<?>, MyJavaEncoder<?>> encoders = new HashMap<>();
    static {
        encoders.put(String.class, new MyJavaEncoder<String>() {
            @Override
            public String encode(String s) {
                System.out.println("using String encoder for: " + s);
                return s;
            }
        });
        encoders.put(Integer.class, new MyJavaEncoder<Integer>() {
            @Override
            public Integer encode(Integer i) {
                System.out.println("using Integer encoder for: " + i);
                return i;
            }
        });
    }

    private static <R> MyJavaEncoder<R> getEncoder(Class<R> clazz) {
        @SuppressWarnings("unchecked")
        MyJavaEncoder<R> encoder = (MyJavaEncoder<R>) encoders.get(clazz);
        return encoder;
    }

    static abstract class MyJavaEncoder<T> {
        public abstract T encode(T t);
    }

    static class MyJavaDataset<T> {

        private final Stream<T> elements;

        private MyJavaDataset(Stream<T> elements) {
            this.elements = elements;
        }
        private MyJavaDataset(List<T> elements) {
            this(elements.stream());
        }
        public static <T> MyJavaDataset<T> create(List<T> elements) {
           return new MyJavaDataset<>(elements);
        }
        public <R> MyJavaDataset<R> map(Function<T, R> function) {
            MyJavaEncoder<R> encoder = getOutputEncoder(function);
            Stream<R> stream = elements
                    .map(function)
                    .map(encoder::encode);
            return new MyJavaDataset<>(stream);
        }

        public <R> MyJavaDataset<R> map(MyAbstractFunction<T, R> function) {
            MyJavaEncoder<R> encoder = getOutputEncoder(function);
            Stream<R> stream = elements
                    .map(function::apply)
                    .map(encoder::encode);
            return new MyJavaDataset<>(stream);
        }

        private <R> MyJavaEncoder<R> getOutputEncoder(Function<T, R> function) {
            Class<R> outputType = getOutputType(function);
            return getEncoder(outputType);
        }

        public void show() {
            elements.forEach(System.out::println);
        }

        private <R> MyJavaEncoder<R> getOutputEncoder(MyAbstractFunction<T, R> function) {
            Class<R> outputType = function instanceof MapElements ?
                    ((MapElements<T, R>) function).getClazz()
                    :
                    getOutputType(function);
            return getEncoder(outputType);
        }
    }


    public static <T, R> Class<R> getOutputType(Function<T, R> function) {
        if (function instanceof MyParallelCollection.MyAbstractFunction) {
            Type genericSuperclass = function.getClass().getGenericSuperclass();
            if (genericSuperclass != null) {
                if (genericSuperclass instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
                    Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                    System.out.println("function: " + function + " genericSuperclass actualTypeArguments=" + Arrays.asList(actualTypeArguments));
                    if (actualTypeArguments.length > 1) {
                        @SuppressWarnings("unchecked")
                        Class<R> actualTypeArgument = (Class<R>) actualTypeArguments[1];
                        return actualTypeArgument;
                    }
                }
            }
            throw new IllegalArgumentException("Cannot determine actualTypeArguments for function " + function + ", genericSuperclass=" + genericSuperclass);
        }
        Type[] genericInterfaces = function.getClass().getGenericInterfaces();
        if (genericInterfaces.length > 0) {
            Type genericInterface = genericInterfaces[0];
            if (genericInterface instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) genericInterface;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                System.out.println("function: " + function + " genericInterfaces[0] actualTypeArguments=" + Arrays.asList(actualTypeArguments));
                if (actualTypeArguments.length > 1) {
                    @SuppressWarnings("unchecked")
                    Class<R> actualTypeArgument = (Class<R>) actualTypeArguments[1];
                    return actualTypeArgument;
                }
            }
        }
        throw new IllegalArgumentException("Cannot determine for function " + function + " , actualTypeArguments, genericInterfaces=" + Arrays.asList(genericInterfaces));
    }
}
