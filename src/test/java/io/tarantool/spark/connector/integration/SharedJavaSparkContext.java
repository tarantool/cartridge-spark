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
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexey Kuzin
 */
public abstract class SharedJavaSparkContext {

    private static final Logger logger = LoggerFactory.getLogger(SharedJavaSparkContext.class);
    private static final String clusterCookie =
        System.getenv().getOrDefault("TARANTOOL_CLUSTER_COOKIE", "testapp-cluster-cookie");
    private static final Map<String, String> buildArgs =
        Collections.singletonMap("TARANTOOL_CLUSTER_COOKIE", clusterCookie);
    private static final String instancesFileName =
        System.getenv().getOrDefault("TARANTOOL_INSTANCE_FILE", "cartridge/instances.yml");
    private static final String topologyFileName =
        System.getenv().getOrDefault("TARANTOOL_TOPOLOGY_FILE", "cartridge/topology.lua");

    protected static final TarantoolCartridgeContainer container =
            new TarantoolCartridgeContainer(
                    "Dockerfile",
                    "tarantool-spark-test",
                    instancesFileName,
                    topologyFileName,
                    buildArgs)
                    .withDirectoryBinding("cartridge")
                    .withRouterPassword(clusterCookie)
                    .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 2))
                    .withStartupTimeout(Duration.ofMinutes(10))
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
                .config("spark.ui.enabled", false)
                .config(confWithTarantoolProperties(container.getRouterPort()))
                .getOrCreate();
    }

    private static SparkConf confWithTarantoolProperties(Integer routerPort) {
        SparkConf _conf = new SparkConf(false)
                .setMaster(master)
                .setAppName(appName);
        _conf.set("tarantool.username", "admin");
        _conf.set("tarantool.password", clusterCookie);
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
