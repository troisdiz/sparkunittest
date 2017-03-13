package com.troisdizaines.sparkunittest.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.ExternalResource;

public class SparkCoreRule extends ExternalResource {

    private JavaSparkContext sc;

    public SparkCoreRule() {
        super();
    }

    @Override
    protected void before() throws Throwable {
        sc = SparkTester.INSTANCE.startUsing();
    }

    @Override
    protected void after() {
        sc = null;
        SparkTester.INSTANCE.stopUsing();
    }

    public JavaSparkContext getSparkContext() {
        return sc;
    }
}
