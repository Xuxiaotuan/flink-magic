package cn.xuyinyin.flink.magic.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author : XuJiaWei
 * @since : 2023-06-07 18:19
 */
public class Example {

    public static void main(String[] args) throws Exception {

        // init stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2)
        );


        // transform
        DataStream<Person> adults = flintstones
                .filter((FilterFunction<Person>) person -> person.age >= 18);

        // sink
        adults.print();

        // execute
        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person() {
        }

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }
}
