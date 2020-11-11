# cartridge-spark

Apache Spark connector for Tarantool and Tarantool Cartridge

## Building

Clone this project and build it using [sbt](https://www.scala-sbt.org/) (just run command `sbt test`).

## Linking

You can link against this library (for Spark 2.2) in your program at the following coordinates:

```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-spark</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

or (for Scala)

```
libraryDependencies += "io.tarantool" %% "cartridge-spark" % "1.0.0-SNAPSHOT"
```

## Version Compatibility

| Connector | Apache Spark | Tarantool Server |
| --------- | ------------ | ---------------- |
| 1.x.x     | 2.2          | 1.10.x,  2.2.x-2.4.x   |

## Getting Started

### Configuration

| property-key                            | description                                 | default value   |
| --------------------------------------- | ------------------------------------------- | --------------- |
| tarantool.hosts                         | comma separated list of Tarantool hosts                   | 127.0.0.1:3301  |
| tarantool.username                      | basic authentication user                                 | guest           |
| tarantool.password                      | basic authentication password                             |                 |
| tarantool.connectTimeout                | server connect timeout, in milliseconds                   | 1000            |
| tarantool.readTimeout                   | socket read timeout, in milliseconds                      | 1000            |
| tarantool.requestTimeout                | request completion timeout, in milliseconds               | 2000            |
| tarantool.useProxyClient                | use ProxyTarantoolClient for working with [tarantool/crud](https://github.com/tarantool/crud) | false           |
| tarantool.useClusterDiscovery           | use cluster discovery (dynamic receiving of the list of hosts) | false           |
| tarantool.discoveryProvider             | service discovery provider ("http" or "binary")           |                 |
| tarantool.discoveryConnectTimeout       | service discovery connect timeout, in ms                  | 1000            |
| tarantool.discoveryReadTimeout          | service discovery read timeout, in ms                     | 1000            |
| tarantool.discoveryDelay                | cluster discovery delay, ms                               | 60000           |
| tarantool.discoveryHttpUrl              | discovery endpoint URI                                    |                 |
| tarantool.discoveryBinaryEntryFunction  | binary discovery function name                            |                 |
| tarantool.discoveryBinaryHost           | binary discovery Tarantool host address                   |                 |
  

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

  val rdd = TarantoolSpark.load[TarantoolTuple](sc, "test_space")
```

```java
    SparkConf conf = new SparkConf()
    JavaSparkContext sc = new JavaSparkContext(conf);

    TarantoolJavaRDD[TarantoolTuple] rdd = TarantoolSpark.load[TarantoolTuple](sc, "test_space")
```

## Learn more

- [Tarantool](https://www.tarantool.io/)