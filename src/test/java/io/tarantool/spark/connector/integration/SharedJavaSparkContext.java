package io.tarantool.spark.connector.integration;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
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

    protected static AtomicReference<JavaSparkContext> jsc = new AtomicReference<>();
    private static String master = "local";
    private static String appName = "tarantool-spark-test";

    @BeforeClass
    public static void beforeAll() {
        startCluster();
        if (jsc.get() == null) {
            jsc.compareAndSet(null, new JavaSparkContext(getSparkContext()));
        }
    }

    private static SparkContext getSparkContext() {
        return new SparkContext(confWithTarantoolProperties(container.getRouterPort()));
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

    @AfterClass
    public static void afterAll() {
        JavaSparkContext scRef = jsc.get();
        if (jsc.compareAndSet(scRef, null)) {
            scRef.stop();
        }
        container.stop();
    }
}
