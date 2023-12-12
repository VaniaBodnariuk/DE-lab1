package org.example;

public class App {
    public static void main( String[] args ) {
        SparkProcessor processor = new SparkProcessor(new SparkConfig(), new FileWriter());
        processor.processData("10K.github.jsonl");
    }
}
