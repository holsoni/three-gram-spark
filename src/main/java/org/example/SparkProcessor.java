package org.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.ArrayList;

public class SparkProcessor {
    private SparkSession spark;
    public void processData(String filepath) {
        spark = SparkSession
                .builder()
                .appName("Java Spark CSV Example")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> data = readData(spark, filepath);

        data = filterData(data);
        data = processRows(data);
        List<Row> list = data.collectAsList();
        FileWriterUtil.writeToFile("results.txt", list);
    }

    private Dataset<Row> readData(SparkSession spark, String filepath) {
        return spark.sqlContext().read().format("com.databricks.spark.csv")
                .load(filepath);
    }

    private Dataset<Row> filterData(Dataset<Row> data) {
        return data.filter("_c1 = '\"type\":\"PushEvent\"' AND _c20 LIKE '%message%'")
                .select("_c3", "_c20");
    }

    private Dataset<Row> processRows(Dataset<Row> data) {
        spark.udf().register("extractNameAndMessages", (String login, String message) -> {
            String name = login.split(":")[1].replace("\"", "").trim();

            String[] words = message.toLowerCase().replaceAll("[^a-z ]", "").split("\\s+");
            List<String> threeGrams = new ArrayList<>();

            for (int i = 0; i < words.length - 2; i++) {
                String threeGram = words[i] + " " + words[i + 1] + " " + words[i + 2];
                threeGrams.add(threeGram);
            }

            StringBuilder result = new StringBuilder(name);
            for (String threeGram : threeGrams) {
                result.append(", ").append(threeGram);
            }

            return result.toString();
        }, DataTypes.StringType);

        data = data.withColumn("name_and_messages", functions.callUDF("extractNameAndMessages",
                data.col("_c3"),
                data.col("_c20")));

        data = data.select(data.col("name_and_messages"));

        return data;
    }

}
