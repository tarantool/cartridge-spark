package io.tarantool.spark.connector.integration;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.spark.connector.rdd.SparkContextJavaFunctions;
import io.tarantool.spark.connector.rdd.converter.FunctionBasedTupleConverterFactory;
import io.tarantool.spark.connector.rdd.converter.TupleConverterFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static io.tarantool.spark.connector.util.ScalaToJavaHelper.getClassTag;
import static io.tarantool.spark.connector.util.ScalaToJavaHelper.toScalaFunction1;
import static org.junit.Assert.assertTrue;

/**
 * @author Alexey Kuzin
 */
public class JavaSparkContextFunctionsTest extends SharedJavaSparkContext {

    @Before
    public void beforeEach() {
        try {
            container.executeScript("test_setup.lua").get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up test: ", e);
        }
    }

    @After
    public void afterEach() {
        try {
            container.executeScript("test_teardown.lua").get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up test: ", e);
        }
    }

    @Test
    public void testLoadTheWholeSpace() {
        SparkContextJavaFunctions sparkContextFunctions = new SparkContextJavaFunctions(jsc.get().sc());
        List<TarantoolTuple> tuples = sparkContextFunctions
                .tarantoolSpace("test_space", Conditions.any()).collect();

        assertTrue(tuples.size() > 0);
    }

    @Test
    public void testLoadTheWholeSpaceWithTupleConverter() {
        SparkContextJavaFunctions sparkContextFunctions = new SparkContextJavaFunctions(jsc.get().sc());
        TupleConverterFactory<Book> converterFactory = new FunctionBasedTupleConverterFactory<>(
                toScalaFunction1(t -> {
                    Book book = new Book();
                    book.id = t.getInteger("id");
                    book.name = t.getString("name");
                    book.author = t.getString("author");
                    book.year = t.getInteger("year");
                    return book;
                }),
                getClassTag(Book.class)
        );
        List<Book> tuples = sparkContextFunctions
                .tarantoolSpace("test_space", Conditions.any(), converterFactory).collect();

        assertTrue(tuples.size() > 0);
    }
}
