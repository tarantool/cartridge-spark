package io.tarantool.spark.connector.rdd;

import org.apache.spark.api.java.JavaRDD;
import scala.reflect.ClassTag;

import static io.tarantool.spark.connector.util.ScalaToJavaHelper.getClassTag;

/**
 * Bridge from Scala {@link TarantoolRDD} to Java.
 *
 * Instances of this class may be instantiated using methods of {@link SparkContextJavaFunctions}.
 *
 * @param <R> target POJO type
 */
public class TarantoolJavaRDD<R> extends JavaRDD<R> {

    public TarantoolJavaRDD(TarantoolRDD<R> rdd, Class<R> clazz) {
        super(rdd, getClassTag(clazz));
    }

    public TarantoolJavaRDD(TarantoolRDD<R> rdd, ClassTag<R> classTag) {
        super(rdd, classTag);
    }

    @Override
    public TarantoolRDD<R> rdd() {
        return (TarantoolRDD<R>) super.rdd();
    }
}
