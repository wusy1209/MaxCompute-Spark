package com.aliyun.odps.spark.examples.oss2odps;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;


public class JavaSparkCSV {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkOss2ODPS  Oss on Maxcomputer")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

        String pathIn = "oss://danger812/demo/" + args[0] + "/"; // 填写具体的OSS Bucket路径。
        JavaRDD<String> rdd= sparkContext.textFile(pathIn, 5);
        JavaRDD<Row> dfJavaRDD= rdd.map(new Function<String, Row>(){
            public Row call(String line) {
                return RowFactory.create(
                        line.split(","));
            }
        });

        List<StructField> structFilelds = new ArrayList<StructField>();
        structFilelds.add(DataTypes.createStructField("login_user", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("user_name", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("created_time", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("order_type", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("secondary_department", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("third_department", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("order_name", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("secondary_directory", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("primary_directory", DataTypes.StringType, true));
        structFilelds.add(DataTypes.createStructField("event_name", DataTypes.StringType, true));
        Dataset<Row> df = spark.createDataFrame(dfJavaRDD, DataTypes.createStructType(structFilelds));

        String tableName = "dw_bi_tableau_logs";
        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql("CREATE TABLE IF NOT EXISTS "+ tableName + "\n" +
                "(\n" +
                "    login_user           STRING   COMMENT '登录名',\n" +
                "    user_name            STRING   COMMENT '用户名',\n" +
                "    created_time         STRING COMMENT '发生时间',\n" +
                "    order_type           STRING   COMMENT '功能模块',\n" +
                "    secondary_department STRING   COMMENT '二级部门',\n" +
                "    third_department     STRING   COMMENT '三级部门',\n" +
                "    order_name           STRING   COMMENT '菜单',\n" +
                "    secondary_directory  STRING   COMMENT '二级目录',\n" +
                "    primary_directory    STRING   COMMENT '一级目录',\n" +
                "    event_name           STRING   COMMENT '事件名称'\n" +
                ") \n" +
                "lifecycle 2");
        df.write().mode("overwrite").insertInto(tableName);// insertOverwrite语义

        spark.sql("SELECT * FROM " + tableName).show();

    }
}
