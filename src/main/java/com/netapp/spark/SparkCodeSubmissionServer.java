package com.netapp.spark;

import org.apache.spark.sql.SparkSession;

public class SparkCodeSubmissionServer implements AutoCloseable {
    SparkSession spark;

    public SparkCodeSubmissionServer(String master) {
        spark = SparkSession.builder().master(master).appName("SparkCodeSubmissionServer").getOrCreate();
    }

    public void start() {
        var server = new SparkCodeSubmissionDriverPlugin(9002);
        server.init(spark.sparkContext(), null);
    }

    public static void main(String[] args) {
        try (var server = new SparkCodeSubmissionServer(args[0])) {
            server.start();
        }
    }


    @Override
    public void close() {
        spark.close();
    }
}
