# tarantool-spark-connector
Tarantool connector for Apache Spark

## Linking
You can link against this library (for Spark 2.2) in your program at the following coordinates:
```
groupId: io.tarantool
artifactId: tarantool-spark-connector_2.11
version: 2.2.0
```

```
libraryDependencies += "io.tarantool" %% "tarantool-spark-connector" % "2.2.0"
```

## Version Compatibility

| Connector | Apache Spark | Tarantool Server |
| --------- | ------------ | ---------------- |
| 2.2.x     | 2.2          | 1.1.x - 2.x      |

## Getting Started

### Configuration

| property-key                            | description                                 | default value   |
| --------------------------------------- | ------------------------------------------- | --------------- |
| tarantool.hosts                         | comma separated list of Tarantool hosts     | 127.0.0.1:3301  |
| tarantool.username                      | basic authentication user                   | guest           |
| tarantool.password                      | basic authentication password               |                 |
| tarantool.connectTimeout                | server connect timeout, in milliseconds     | 1000            |
| tarantool.readTimeout                   | socket read timeout, in milliseconds        | 1000            |
| tarantool.requestTimeout                | request completion timeout, in milliseconds | 2000            |
| tarantool.useClusterClient              | use TarantoolClusterClient                  | false           |
| tarantool.discoveryProvider             | service discovery provider (http or binary) |                 |
| tarantool.discoverConnectTimeout        | service discovery connect timeout, in ms    | 1000            |  
| tarantool.discoveryReadTimeout          | service discovery read timeout, in ms       | 1000            |
| tarantool.discoveryDelay                | cluster discovery delay, ms                 | 60000           |
| tarantool.discoveryHttpUrl              | discovery endpoint URI                      |                 |
| tarantool.discoveryBinaryEntryFunction  | binary discovery function                   |                 |
| tarantool.discoveryBinaryHost           | binary discovery tarantool host address     |                 |
  

### Setup SparkContext
```scala
val conf = new SparkConf()
    .set("tarantool.hosts", "127.0.0.1:3301")
    .set("tarantool.user", "admin")
    .set("tarantool.password", "password")
    ...

val sc = new SparkContext(conf)
```

```java
SparkConf conf = new SparkConf()
    .set("tarantool.hosts", "127.0.0.1:3301")
    .set("tarantool.user", "admin")
    .set("tarantool.password", "password");
    ...

JavaSparkContext sc = new JavaSparkContext(conf);
```

### Tarantool.load
```
    TarantoolSpark.load[T](sparkContext: JavaSparkContext, space: String, clazz: Class[T]): TarantoolJavaRDD[T]
```

#### Example
```scala
  val conf = new SparkConf()
            ...
  val sc = new SparkContext(conf)

  val rdd = TarantoolSpark.load[TarantoolTuple](sc, "_spark_test_space")
```

```java
    SparkConf conf = new SparkConf()
    JavaSparkContext sc = new JavaSparkContext(conf);

    TarantoolJavaRDD[TarantoolTuple] rdd = TarantoolSpark.load[TarantoolTuple](sc, "_spark_test_space")
```

## Learn more

- [Tarantool](https://www.tarantool.io/)