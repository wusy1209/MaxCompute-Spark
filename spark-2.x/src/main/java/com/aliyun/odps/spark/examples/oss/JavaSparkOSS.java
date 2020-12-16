package com.aliyun.odps.spark.examples.oss;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaSparkOSS {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkOss on Maxcomputer")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        String pathIn = "oss://danger812/demo/" + args[0] + "/"; // 填写具体的OSS Bucket路径。
        JavaRDD<String>  rdd= sparkContext.textFile(pathIn, 5);
        System.out.println("======" + rdd.count());

    }
}
