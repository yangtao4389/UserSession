package cn.angetech.mockData;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class PersonTest {
    public static void main(String[] args) {
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);
        SparkSession spark = SparkSession.builder().appName("java spark sql basic example").config("spark.some.config.option","some value").getOrCreate();
// Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
// +---+----+
// |age|name|
// +---+----+
// | 32|Andy|
// +---+----+

// Encoders for most common types are provided in class Encoders
//        Encoder<Integer> integerEncoder = Encoders.INT();
//        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
//        Dataset<Integer> transformedDS = primitiveDS.map(
//                (MapFunction<Integer, Integer>) value -> value + 1,
//                integerEncoder);
//        transformedDS.collect(); // Returns [2, 3, 4]

// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "examples/src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
    }
}
