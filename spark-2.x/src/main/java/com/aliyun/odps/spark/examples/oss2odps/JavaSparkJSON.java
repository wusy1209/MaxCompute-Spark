package com.aliyun.odps.spark.examples.oss2odps;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.spark.examples.models.ItemBase;
import com.aliyun.odps.spark.examples.models.ItemJeyooTopic;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.seimicrawler.xpath.JXDocument;


public class JavaSparkJSON {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkJSON on Maxcomputer")
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        String pathIn = "oss://danger812/jeyoo/" + args[0] + "/"; // 填写具体的OSS Bucket路径。
        Encoder<ItemBase> itemBaseEncoder = Encoders.bean(ItemBase.class);
        Dataset<ItemBase> itemDS= spark.read().json(pathIn).as(itemBaseEncoder);
//        df.printSchema();
//        df.show();
//        df.map((MapFunction<Item, Integer>) item -> {
//            return 1;
//        }, Encoders.INT());
        System.out.println(itemDS.count());

        Encoder<ItemJeyooTopic> itemJeyooTopicEncoder = Encoders.bean(ItemJeyooTopic.class);
        JavaRDD<ItemJeyooTopic> itemJavaRDD= itemDS.toJavaRDD().map(item -> {
            JXDocument jxDocument = JXDocument.create(item.getContent());
            String tmp_question1 = String.valueOf(jxDocument.selOne("//div[@class=\"pt1\"]"));
            String tmp_question2 = String.valueOf(jxDocument.selOne("//div[@class=\"pt2\"]"));
            String question = tmp_question1 + tmp_question2;
            String knowledge = String.valueOf(jxDocument.selOne("//div[@class=\"pt3\"]"));
            String answer = String.valueOf(jxDocument.selOne("//div[@class=\"pt6\"]"));
//            JSONObject pp= JSONObject.parseObject(item.getPp());
            ItemJeyooTopic itemJeyooTopic= new ItemJeyooTopic();
            itemJeyooTopic.setFathor(item);
            itemJeyooTopic.setQuestion(question);
            itemJeyooTopic.setKnowledge(knowledge);
            itemJeyooTopic.setAnswer(answer);
            return itemJeyooTopic;
        });
        Dataset<ItemJeyooTopic> itemJeyooTopicDS = spark.createDataset(itemJavaRDD.rdd(), itemJeyooTopicEncoder);

        String tableName = "ods_jy_topic";
        spark.sql("DROP TABLE IF EXISTS " + tableName);
        spark.sql("CREATE TABLE IF NOT EXISTS "+ tableName + "\n" +
                "(\n" +
                "    question           STRING   COMMENT '题目' ,\n" +
                "    knowledge            STRING   COMMENT '知识点',\n" +
                "    answer         STRING COMMENT '答案(含解析)'\n" +
                ") \n" +
                "lifecycle 1");

        itemJeyooTopicDS.select("question", "knowledge", "answer").write().mode("overwrite").insertInto(tableName);// insertOverwrite语义

        spark.sql("SELECT * FROM " + tableName).show();
  }
}
