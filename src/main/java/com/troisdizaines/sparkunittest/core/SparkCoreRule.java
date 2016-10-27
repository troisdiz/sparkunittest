package com.troisdizaines.sparkunittest.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.ExternalResource;

public class SparkCoreRule extends ExternalResource {

    private JavaSparkContext sc;

    public SparkCoreRule() {
        super();
    }

    @Override
    protected void before() throws Throwable {
        SparkConf sparkConfig = new SparkConf();
        sparkConfig.set("spark.broadcast.compress", "false");
        sparkConfig.set("spark.shuffle.compress", "false");
        sparkConfig.set("spark.shuffle.spill.compress", "false");
        sparkConfig.set("spark.io.compression.codec", "lzf");
        sparkConfig.set("spark.ui.enabled", "false");

        sc = new JavaSparkContext("local[2]", "unit test", sparkConfig);
    }

    @Override
    protected void after() {
        sc.stop();
    }

    public JavaSparkContext getSparkContext() {
        return sc;
    }
}
