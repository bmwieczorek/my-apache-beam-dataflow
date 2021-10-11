package com.bawi.beam.dataflow;

public class MyGenericTest {
    static abstract class Fn<In, Out> {
        abstract Out apply(In in);
    }

    static class ObjectToIntegerFn extends Fn<Object, Integer>{
        @Override
        Integer apply(Object object) {
            return null;
        }
    }

    static class MyWindow<T> {
        static <T> MyWindow<T> m1T(Fn<T, ?> fn) {
            return new MyWindow<>();
        }
        static <T> MyWindow<T> m1SuperT(Fn<? super T, ?> fn) {
            return new MyWindow<>();
        }

        MyWindow<T> m2() {
            return new MyWindow<>();
        }
        MyWindow<T> m3() {
            return new MyWindow<>();
        }
    }


    public static void main(String[] args) {

        MyWindow<Object> tObject = MyWindow.m1T(new ObjectToIntegerFn());

//  incompatible types: inference variable T has incompatible equality constraints java.lang.String,java.lang.Object
//        MyWindow<String> tString = MyWindow.m1T(new ObjectToIntegerFn());

        MyWindow<Object> superTObject = MyWindow.m1SuperT(new ObjectToIntegerFn());

        MyWindow<String> superTString = MyWindow.m1SuperT(new ObjectToIntegerFn());

        MyWindow<Object> tObjectObject = tObject.m2();
        MyWindow<Object> superTObjectObject = superTObject.m2(); // allowed
        MyWindow<String> superTStringString = superTString.m2(); // allowed

        MyWindow<Object> tObjectM2 =  MyWindow.m1T(new ObjectToIntegerFn()).m2();
        MyWindow<Object> superTObjectM2 =  MyWindow.m1SuperT(new ObjectToIntegerFn()).m2();

// incompatible types: com.bawi.beam.dataflow.MyGenericTest.MyWindow<java.lang.Object> cannot be converted to com.bawi.beam.dataflow.MyGenericTest.MyWindow<java.lang.String>
//        MyWindow<String> superTStringM2 = MyWindow.m1SuperT(new ObjectToIntegerFn()).m2();
        MyWindow<String> superTStringM2Generic = MyWindow.<String>m1SuperT(new ObjectToIntegerFn()).m2();

        Class<Object> objectClass = get();
        Class<Object> print = print(objectClass);
        Class<String> stringClass = MyGenericTest.get();
        Class<String> print1 = print(stringClass);
        Class<String> stringClass1 = get(String.class);
        Class<String> stringClass2 = get(new Fn<String, Integer>() {
            @Override
            Integer apply(String s) {
                return null;
            }
        });
    }

    static <T> Class<T> print(Class<T> clazz) {
        System.out.println(clazz);
        return clazz;
    }

    static <T> Class<T> get() {
        return null;
    }

    static <T> Class<T> get(Class<T> t) {
        return null;
    }

    static <T> Class<T> get(Fn<T, ?> fn) {
        return null;
    }
}
