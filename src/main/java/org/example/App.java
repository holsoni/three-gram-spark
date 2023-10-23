package org.example;

/**
 * Hello world!
 * 10K.github.jsonl
 *
 */
public class App 
{
    public static void main( String[] args ) {
        SparkProcessor processor = new SparkProcessor();
        processor.processData("10K.github.jsonl");
    }
}
