package io.tarantool.spark.connector;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.spark.connector.config.ReadConfig;
import io.tarantool.spark.connector.rdd.TarantoolJavaRDD;
import io.tarantool.spark.connector.rdd.TarantoolRDD;
import io.tarantool.spark.connector.rdd.TarantoolRDD$;
import io.tarantool.spark.connector.rdd.converter.FunctionBasedTupleConverterFactory;
import io.tarantool.spark.connector.rdd.converter.TupleConverterFactory;
import org.apache.spark.SparkContext;

import java.io.Serializable;
import java.util.function.Function;

import static io.tarantool.spark.connector.util.ScalaToJavaHelper.getClassTag;
import static io.tarantool.spark.connector.util.ScalaToJavaHelper.toScalaFunction1;

/**
 * Java API for bridging {@link SparkContextFunctions} functionality into Java code
 *
 * @author Alexey Kuzin
 */
public class SparkContextJavaFunctions {

    private final SparkContext sparkContext;

    public SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    /**
     * Converts {@link TarantoolRDD} into {@link TarantoolJavaRDD}
     *
     * @param rdd         TarantoolRDD instance
     * @param targetClass target POJO class
     * @param <T>         target POJO type
     * @return TarantoolJavaRDD instance
     */
    public <T> TarantoolJavaRDD<T> toJavaRDD(TarantoolRDD<T> rdd, Class<T> targetClass) {
        return new TarantoolJavaRDD<>(rdd, targetClass);
    }

    /**
     * Load data from a Tarantool space, filtering them with the specified conditions. The resulting RDD is
     * filled with {@link TarantoolTuple}.
     *
     * <p>Example:
     * <pre>
     * local crud = require('crud')
     *
     * crud.insert('test_space', {1, nil, 'a1', 'Don Quixote', 'Miguel de Cervantes', 1605})
     * crud.insert('test_space', {2, nil, 'a2', 'The Great Gatsby', 'F. Scott Fitzgerald', 1925})
     * crud.insert('test_space', {3, nil, 'a3', 'War and Peace', 'Leo Tolstoy', 1869})
     * ...
     *
     * TarantoolJavaRDD<TarantoolTuple> rdd = TarantoolSpark.contextFunctions(jsc)
     *      .tarantoolSpace("test_space", Conditions.indexGreaterThan("id", Collections.singletonList(1)));
     * rdd.first().getInteger("id"); // 1
     * rdd.first().getString("author"); // "Miguel de Cervantes"
     * </pre>
     * </p>
     *
     * @param spaceName  Tarantool space name
     * @param conditions space filtering conditions
     * @return instance of {@link TarantoolRDD}
     */
    public TarantoolJavaRDD<TarantoolTuple> tarantoolSpace(String spaceName, Conditions conditions) {
        TarantoolRDD<TarantoolTuple> rdd = TarantoolRDD$.MODULE$.apply(
                sparkContext, spaceName, conditions,
                ReadConfig.fromSparkConf(sparkContext.getConf()),
                getClassTag(TarantoolTuple.class));
        return new TarantoolJavaRDD<>(rdd, TarantoolTuple.class);
    }

    /**
     * Load data from a Tarantool space, filtering them with the specified conditions.
     * <p/>
     * Tarantool tuples are converted into the target entity type using a converter provided by the
     * specified tuple converter factory.
     *
     * @param spaceName             Tarantool space name
     * @param conditions            space filtering conditions
     * @param tupleConverterFactory provides the converter of {@link TarantoolTuple} into the target entity type {@code R}
     * @param <R>                   target entity type
     * @return instance of {@link TarantoolRDD}
     */
    public <R> TarantoolJavaRDD<R> tarantoolSpace(String spaceName, Conditions conditions,
                                                  TupleConverterFactory<R> tupleConverterFactory) {
        TarantoolRDD<R> rdd = TarantoolRDD$.MODULE$.apply(
                sparkContext, spaceName, conditions,
                ReadConfig.fromSparkConf(sparkContext.getConf()),
                tupleConverterFactory.tupleConverter(),
                getClassTag(tupleConverterFactory.targetClass()));
        return new TarantoolJavaRDD<>(rdd, tupleConverterFactory.targetClass());
    }

    /**
     * Load data from Tarantool space.
     * <p/>
     * The resulting tuples are converted into instances of the specified type using the provided converter.
     *
     * @param spaceName      Tarantool space name
     * @param conditions     filtering conditions for space
     * @param tupleConverter custom converter from Tarantool tuples to a target entity class
     * @param targetClass    target entity class
     * @param <R>            target entity type
     * @return instance of {@link TarantoolJavaRDD}
     */
    public <R> TarantoolJavaRDD<R> tarantoolSpace(String spaceName, Conditions conditions,
                                                  SerializableFunction<TarantoolTuple, R> tupleConverter,
                                                  Class<R> targetClass) {
        TupleConverterFactory<R> converterFactory = new FunctionBasedTupleConverterFactory<>(
                toScalaFunction1(tupleConverter),
                getClassTag(targetClass)
        );

        return tarantoolSpace(spaceName, conditions, converterFactory);
    }

    public interface SerializableFunction<E, R> extends Function<E, R>, Serializable {
    }
}
