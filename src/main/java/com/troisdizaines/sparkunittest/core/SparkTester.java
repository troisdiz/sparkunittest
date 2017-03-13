package com.troisdizaines.sparkunittest.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public enum SparkTester {

    INSTANCE;

    private JavaSparkContext sc;
    private int refCount;

    private SparkTester() {
        refCount = 0;
    }

    public synchronized JavaSparkContext startUsing() {
        if (refCount == 0) {
            startSpark();
        }
        refCount++;
        return sc;
    }

    public void stopUsing() {
        if (refCount == 1) {
            refCount = 0;
            stopSpark();
        }
    }

    private synchronized void stopSpark() {
        if (sc != null) {
            sc.stop();
            sc = null;
        }
    }

    private synchronized void startSpark() {
        if (sc == null) {
            SparkConf sparkConfig = new SparkConf();
            sparkConfig.set("spark.broadcast.compress", "false");
            sparkConfig.set("spark.shuffle.compress", "false");
            sparkConfig.set("spark.shuffle.spill.compress", "false");
            sparkConfig.set("spark.io.compression.codec", "lzf");
            sparkConfig.set("spark.ui.enabled", "false");

            sc = new JavaSparkContext("local[2]", "unit test", sparkConfig);
        }
    }

}
