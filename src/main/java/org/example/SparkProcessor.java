package org.example;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

@RequiredArgsConstructor
public class SparkProcessor {
    private static final String EXTRACT_NAME_WITH_3GRAM_MESSAGES_FUNCTION = "extractNameAndMessages";
    private static final String RESULTS_FILE_PATH = "results.txt";
    private static final String SPARK_CSV = "com.databricks.spark.csv";
    private static final String PUSH_EVENT_CONDITION = "_c1 = '\"type\":\"PushEvent\"' AND _c20 LIKE '%message%'";
    private static final String NAME_COLUMN = "_c3";
    private static final String MESSAGES_COLUMN = "_c20";
    private static final String NAME_AND_MESSAGES_COLUMN = "name_and_messages";

    private final SparkConfig sparkConfig;
    private final FileWriter fileWriter;

    public void processData(String filepath) {
        try (SparkSession session = sparkConfig.getSession()) {
            Dataset<Row> data = readDataFrom(filepath, session);
            data = filterPushedEvents(data);
            data = transformToNameWith3GramMessages(data, session);
            fileWriter.writeTo(RESULTS_FILE_PATH, data.collectAsList());
        }
    }

    private Dataset<Row> readDataFrom(String filepath, SparkSession spark) {
        return spark.sqlContext().read().format(SPARK_CSV).load(filepath);
    }

    private Dataset<Row> filterPushedEvents(Dataset<Row> data) {
        return data.filter(PUSH_EVENT_CONDITION)
                .select(NAME_COLUMN, MESSAGES_COLUMN);
    }

    private Dataset<Row> transformToNameWith3GramMessages(Dataset<Row> data, SparkSession session) {
        session.udf().register(
                EXTRACT_NAME_WITH_3GRAM_MESSAGES_FUNCTION,
                (String login, String message) -> extractNameAnd3GramMessages(login, message),
                DataTypes.StringType);

        data = data.withColumn(NAME_AND_MESSAGES_COLUMN, functions.callUDF(EXTRACT_NAME_WITH_3GRAM_MESSAGES_FUNCTION,
                data.col(NAME_COLUMN),
                data.col(MESSAGES_COLUMN)));

        data = data.select(data.col(NAME_AND_MESSAGES_COLUMN));

        return data;
    }

    private static String extractNameAnd3GramMessages(String login, String message) {
        String name = login.split(":")[1].replace("\"", "").trim();
        String[] words = message.toLowerCase().replaceAll("[^a-z ]", "").split("\\s+");

        StringBuilder result = new StringBuilder(name);
        for (int i = 0; i < words.length - 2; i++) {
            String threeGram = words[i] + " " + words[i + 1] + " " + words[i + 2];
            result.append(", ").append(threeGram);
        }

        return result.toString();
    }
}
