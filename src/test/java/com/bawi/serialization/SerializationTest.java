package com.bawi.serialization;

import org.junit.Assert;
import org.junit.Test;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

@FunctionalInterface
interface MySerFuncIface<T> extends Serializable {
    T apply(T t);
}

class MyNonSerializable {}
class MySerializable implements Serializable {
    MyNonSerializable myNonSerializable;
    public void init() { // executed after deserialization
        myNonSerializable = new MyNonSerializable();
    }
    public String apply(String input) {
        return input + ":" + myNonSerializable;
    }
}

public class SerializationTest {
    // static nested classes behave same as standalone classes
    static class MyStaticNestedNonSerializable {}
    static class MyStaticNestedSerializable implements Serializable {}

    MyNonSerializable fieldMyNonSerializable = new MyNonSerializable();
    MySerializable fieldMySerializable = new MySerializable();

    // static fields are not serialized (only instance fields)
    static MyNonSerializable staticMyNonSerializable = new MyNonSerializable();
    static MySerializable staticMySerializable = new MySerializable();

    class MyInnerNonSerializable {}
    class MyInnerSerializable implements Serializable {}

    //static MyInnerNonSerializable staticMyInnerNonSerializable = new MyInnerNonSerializable(); // Error: java: non-static variable this cannot be referenced from a static context
    MyInnerNonSerializable fieldMyInnerNonSerializable = new MyInnerNonSerializable();

    MyInnerSerializable fieldMyInnerSerializable = new MyInnerSerializable();

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        assertTrue(serializeWithNotSerExMsg(new MyNonSerializable()).endsWith("MyNonSerializable"));
        assertTrue(serializeWithNotSerExMsg(new MyStaticNestedNonSerializable()).endsWith("SerializationTest$MyStaticNestedNonSerializable"));
        assertTrue(serializeWithNotSerExMsg(new MyInnerNonSerializable()).endsWith("SerializationTest$MyInnerNonSerializable"));


        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        MySerializable obj = new MySerializable();
        //obj.myNonSerializable = new MyNonSerializable(); // causes NotSerializableException
        try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
            oos.writeObject(obj);
        }
        byte[] bytes = buffer.toByteArray();
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            Object o = objectInputStream.readObject();
            MySerializable mySer = (MySerializable) o;
            mySer.init(); // after deserialization assign myNonSerializable field with new instance of MyNonSerializable
            String result = mySer.apply("a");
            Assert.assertTrue(result.contains("MyNonSerializable"));
        }

                                  serialize(new MySerializable());
                                  serialize(new MyStaticNestedSerializable());
        assertTrue(serializeWithNotSerExMsg(new MyInnerSerializable()).endsWith("SerializationTest"));


        assertTrue(serializeWithNotSerExMsg(((Function<String, String>) s -> s)).contains("SerializationTest$$Lambda$"));

        // in lambda - MyNonSerializable class
                                  serialize(s -> { print(staticMyNonSerializable); return s; });
        assertTrue(serializeWithNotSerExMsg(s -> { print(fieldMyNonSerializable); return s; }).endsWith("SerializationTest"));
        MyNonSerializable localMyNonSerializable = new MyNonSerializable();
        assertTrue(serializeWithNotSerExMsg(s -> { print(localMyNonSerializable); return s; }).endsWith("MyNonSerializable"));
                                  serialize(s -> { print(new MyNonSerializable()); return s; });

        // in lambda - MySerializable class
                                  serialize(s -> { print(staticMySerializable); return s; });
        assertTrue(serializeWithNotSerExMsg(s -> { print(fieldMySerializable); return s; }).endsWith("SerializationTest"));
        MySerializable localMySerializable = new MySerializable();
                                  serialize(s -> { print(localMySerializable); return s; });
                                  serialize(s -> { print(new MySerializable()); return s; });


        // in lambda - MyInnerNonSerializable class
        assertTrue(serializeWithNotSerExMsg(s -> { print(fieldMyInnerNonSerializable); return s; }).endsWith("SerializationTest"));
        MyInnerNonSerializable localMyInnerNonSerializable = new MyInnerNonSerializable();
        assertTrue(serializeWithNotSerExMsg(s -> { print(localMyInnerNonSerializable); return s; }).endsWith("MyInnerNonSerializable"));
        assertTrue(serializeWithNotSerExMsg(s -> { print(new MyInnerNonSerializable()); return s; }).endsWith("SerializationTest"));

        // in lambda - MyInnerNonSerializable class
        assertTrue(serializeWithNotSerExMsg(s -> { print(fieldMyInnerSerializable); return s; }).endsWith("SerializationTest"));
        MyInnerSerializable localMyInnerSerializable = new MyInnerSerializable();
        assertTrue(serializeWithNotSerExMsg(s -> { print(localMyInnerSerializable); return s; }).endsWith("SerializationTest"));
        assertTrue(serializeWithNotSerExMsg(s -> { print(new MyInnerSerializable()); return s; }).endsWith("SerializationTest"));
    }

    private static void serialize(Object o) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(new SnappyOutputStream(buffer))) {
            oos.writeObject(o);
        }
    }

    private static String serializeWithNotSerExMsg(Object o) throws IOException {
        try {
            serialize(o);
            return "";
        } catch (NotSerializableException e) {
            return e.toString();
        }
    }

    private static String serializeWithNotSerExMsg(MySerFuncIface<String> mySerFuncIface) throws IOException {
        return serializeWithNotSerExMsg((Object) mySerFuncIface);
    }

    private static void serialize(MySerFuncIface<String> mySerFuncIface) throws IOException {
        serialize((Object) mySerFuncIface);
    }

    private static void print(Object o) {
        System.out.println(o);
    }
}
