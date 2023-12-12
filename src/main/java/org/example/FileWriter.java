package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;

@Slf4j
public class FileWriter {

    public void writeTo(String filePath, List<Row> lines) {
        try (java.io.FileWriter writer = new java.io.FileWriter(filePath)) {
            for (Row line : lines) {
                writer.write(line.toString() + "\n");
            }
        } catch (IOException e) {
            log.error("Error occurred during file write", e);
        }
    }
}