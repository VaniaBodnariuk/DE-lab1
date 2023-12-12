package org.example;

import lombok.experimental.UtilityClass;
import org.apache.spark.sql.SparkSession;


public class SparkConfig {
    private final SparkSession.Builder SPARK_SESION_BUILDER = SparkSession.builder()
                .appName("Java Spark CSV Example")
                .config("spark.master", "local");

    public SparkSession getSession() {
        return SPARK_SESION_BUILDER.getOrCreate();
    }
}
