/*
 * Copyright (C) by Courtanet, All Rights Reserved.
 */
package com.troisdizaines.sparkunittest.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Rule;
import org.junit.Test;

import scala.Tuple2;

public class SimpleSparkTest {

    @Rule
    public SparkCoreRule sparkRule = new SparkCoreRule();

    @Test
    public void testSpark() {
        List<String> strs = Arrays.asList(
                        "Courage is not simply one of the virtues, but the form of every virtue at the testing point",
                        "We have a very active testing community which people don't often think about when you have " +
                                        "open source",
                        "Program testing can be used to show the presence of bugs, but never to show their absence",
                        "Simple systems are not feasible because they require infinite testing",
                        "Testing leads to failure, and failure leads to understanding");

        JavaRDD<String> quotesRDD = sparkRule.getSparkContext().parallelize(strs);
        JavaPairRDD<String, Integer> counts = quotesRDD
                        .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                        .mapToPair(w -> new Tuple2<String, Integer>(w.toLowerCase(), 1))
                        .reduceByKey((a, b) -> a + b);

        Map<String, Integer> wordMap = new HashMap<String, Integer>();
        counts.take(100)
                        .forEach(t -> wordMap.put(t._1(), t._2()));
        assertThat(wordMap.get("to")).isEqualTo(4);
        //  "The word count for 'to' should had been 4 but it was " + wordMap.get("to").get)
        assertThat(wordMap.get("testing")).isEqualTo(5);
        // "The word count for 'testing' should had been 5 but it was " + wordMap.get("testing").get)
        assertThat(wordMap.get("is")).isEqualTo(1);
        // "The word count for 'is' should had been 1 but it was " + wordMap.get("is").get)
    }
}
