package io.tarantool.spark.connector.integration;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Alexey Kuzin
 */
public abstract class SharedJavaSparkContext {

    private static final Logger logger = LoggerFactory.getLogger(SharedJavaSparkContext.class);

    protected static final TarantoolCartridgeContainer container =
            new TarantoolCartridgeContainer(
                    "cartridge/instances.yml",
                    "cartridge/topology.lua")
                    .withDirectoryBinding("cartridge")
                    .withLogConsumer(new Slf4jLogConsumer(logger));

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }

    private final static AtomicReference<SparkSession> sparkSession = new AtomicReference<>();
    private final static String master = "local";
    private final static String appName = "tarantool-spark-test";

    @BeforeClass
    public static void beforeAll() {
        startCluster();
        if (sparkSession.get() == null) {
            sparkSession.compareAndSet(null, getSparkSession());
        }
    }

    private static SparkSession getSparkSession() {
        return SparkSession.builder()
                .config(confWithTarantoolProperties(container.getRouterPort()))
                .getOrCreate();
    }

    private static SparkConf confWithTarantoolProperties(Integer routerPort) {
        SparkConf _conf = new SparkConf(false)
                .setMaster(master)
                .setAppName(appName);
        _conf.set("tarantool.username", "admin");
        _conf.set("tarantool.password", "testapp-cluster-cookie");
        _conf.set("tarantool.hosts", "127.0.0.1:" + routerPort);

        return _conf;
    }

    protected static SparkSession spark() {
        return sparkSession.get();
    }

    protected static JavaSparkContext jsc() {
        return new JavaSparkContext(sparkSession.get().sparkContext());
    }

    @AfterClass
    public static void afterAll() {
        SparkSession sessionRef = sparkSession.get();
        if (sparkSession.compareAndSet(sessionRef, null)) {
            sessionRef.stop();
        }
        container.stop();
    }
}
