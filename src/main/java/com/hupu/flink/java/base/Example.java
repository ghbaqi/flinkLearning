package com.hupu.flink.java.base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * source 的种类
 * <p>
 * env.fromCollection(people)
 * <p>
 * env.socketTextStream("localhost", 9999)
 * <p>
 * env.readTextFile("file:///path")
 * <p>
 * kafaka  kinesis 等
 * <p>
 * <p>
 * ***************************************************************
 * <p>
 * sink 的种类
 * <p>
 * stream.writeAsText("/path/to/file")
 * <p>
 * stream.writeAsCsv("/path/to/file")
 * <p>
 * stream.writeToSocket(host, port, SerializationSchema)
 * <p>
 * kafka  databases  hadoop 等
 */


/**
 * 入门级 demo
 */

public class Example {

    public static void main(String[] args) throws Exception {

        // 创建一个执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        // transform
        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        // sink
        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person() {
        }

        ;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        ;

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }

        ;
    }

}
