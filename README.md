# tarantool-spark-connector

Apache Spark connector for Tarantool and Tarantool Cartridge

## Building

Build the project using [sbt](https://www.scala-sbt.org/) (just run command `sbt test`).

## Linking

You can link against this library for Maven in your program at the following coordinates:

```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>tarantool-spark-connector</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

or for `sbt`:

```
libraryDependencies += "io.tarantool" %% "tarantool-spark-connector" % "1.0.0-SNAPSHOT"
```

## Version Compatibility

| Connector | Scala   | Apache Spark | Tarantool Server |
| --------- | ------- | ------------ | ---------------- |
| 1.x.x     | 2.10.7  | 2.2.1        | 1.10.9+,  2.4+   |
| 1.x.x     | 2.11.12 | 2.4.8        | 1.10.9+,  2.4+   |
| 1.x.x     | 2.12.13 | 2.4.8        | 1.10.9+,  2.4+   |

## Getting Started

### Configuration

| property-key                            | description                                          | default value   |
| --------------------------------------- | ---------------------------------------------------- | --------------- |
| tarantool.hosts                         | comma separated list of Tarantool hosts              | 127.0.0.1:3301  |
| tarantool.username                      | basic authentication user                            | guest           |
| tarantool.password                      | basic authentication password                        |                 |
| tarantool.connectTimeout                | server connect timeout, in milliseconds              | 1000            |
| tarantool.readTimeout                   | socket read timeout, in milliseconds                 | 1000            |
| tarantool.requestTimeout                | request completion timeout, in milliseconds          | 2000            |
| tarantool.cursorBatchSize               | default limit for prefetching tuples in RDD iterator | 1000            |

### Setup SparkContext

Using Scala:
```scala

// Using a default client (proxy client for Tarantool Cartridge + tarantool/crud)
val conf = new SparkConf()
    .set("tarantool.hosts", "127.0.0.1:3301")
    .set("tarantool.user", "admin")
    .set("tarantool.password", "password")

val sc = new SparkContext(conf)
```

or Java:
```java
SparkConf conf = new SparkConf()
    .set("tarantool.hosts", "127.0.0.1:3301")
    .set("tarantool.user", "admin")
    .set("tarantool.password", "password");

JavaSparkContext sc = new JavaSparkContext(conf);
```

#### Example

Using Scala:
```scala
  val conf = new SparkConf()
  val sc = new SparkContext(conf)

  // Load the whole space
  val rdd: Array[TarantoolTuple] = sc.get().tarantoolSpace("test_space").collect()

  // Filter using conditions
  val mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
  val startTuple = new DefaultTarantoolTupleFactory(mapper).create(List(1).asJava)
  val cond: Conditions = Conditions
    .indexGreaterThan("id", List(1).asJava)
    .withLimit(2)
    .startAfter(startTuple)
  val rdd: Array[TarantoolTuple] = sc.get().tarantoolSpace("test_space", cond).collect()
```

or Java:
```java
    SparkConf conf = new SparkConf()
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // Use custom tuple conversion
    SparkContextJavaFunctions sparkContextFunctions = new SparkContextJavaFunctions(jsc.sc());
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
```

## Learn more

- [Tarantool](https://www.tarantool.io/)