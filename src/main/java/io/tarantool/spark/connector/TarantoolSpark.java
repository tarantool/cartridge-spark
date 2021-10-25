package io.tarantool.spark.connector;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Public facade for using the Tarantool Spark API in Java.
 * <p/>
 * Provides static factory methods as entrypoints for building RDDs and other Spark API entities.
 *
 * @author Alexey Kuzin
 */
public final class TarantoolSpark {
    /**
     * Get a {@link SparkContextJavaFunctions} entrypoint
     * @param jsc Spark context for Java
     * @return new instance of {@link SparkContextJavaFunctions}
     */
    public static SparkContextJavaFunctions contextFunctions(JavaSparkContext jsc) {
        return new SparkContextJavaFunctions(jsc.sc());
    }

    /**
     * Get a {@link SparkContextJavaFunctions} entrypoint
     * @param sc Spark context
     * @return new instance of {@link SparkContextJavaFunctions}
     */
    public static SparkContextJavaFunctions contextFunctions(SparkContext sc) {
        return new SparkContextJavaFunctions(sc);
    }
}
