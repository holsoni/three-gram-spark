package org.example;

import org.apache.spark.sql.Row;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileWriterUtil {
    public static void writeToFile(String filePath, List<Row> lines) {
        try {
            FileWriter writer = new FileWriter(filePath);
            for (Row line : lines) {
                writer.write(line.toString() + "\n");
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}