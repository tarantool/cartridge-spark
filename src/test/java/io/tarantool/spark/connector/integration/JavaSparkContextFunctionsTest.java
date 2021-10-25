package io.tarantool.spark.connector.integration;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.spark.connector.TarantoolSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
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
        List<TarantoolTuple> tuples = TarantoolSpark.contextFunctions(jsc())
                .tarantoolSpace("test_space", Conditions.any()).collect();

        assertTrue(tuples.size() > 0);
    }

    @Test
    public void testLoadTheWholeSpaceWithTupleConverter() {
        List<Book> tuples = TarantoolSpark.contextFunctions(jsc())
                .tarantoolSpace("test_space", Conditions.any(), t -> {
                    Book book = new Book();
                    book.id = t.getInteger("id");
                    book.name = t.getString("name");
                    book.author = t.getString("author");
                    book.year = t.getInteger("year");
                    return book;
                }, Book.class).collect();

        assertTrue(tuples.size() > 0);
    }

    @Test
    public void testLoadTheWholeSpaceIntoDataFrame() {
        Dataset<Row> ds = spark().read()
                .format("org.apache.spark.sql.tarantool")
                .option("space", "test_space")
                .load();

        assertTrue(ds.count() > 0);
        assertArrayEquals(
                Arrays.asList(1L, 2L, 3L).toArray(),
                ds.select("id").rdd().toJavaRDD().map(row -> row.get(0)).collect().toArray()
        );
    }
}
