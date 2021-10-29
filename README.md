[![Build Status](https://github.com/tarantool/cartridge-spark/workflows/ubuntu-master/badge.svg)](https://github.com/tarantool/cartridge-spark/actions)
[![CodeCov](https://codecov.io/gh/tarantool/cartridge-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/tarantool/cartridge-spark)

# spark-tarantool-connector

Apache Spark connector for Tarantool and Tarantool Cartridge

## Building

Build the project using [sbt](https://www.scala-sbt.org/) (just run command `sbt test`).

## Linking

You can link against this library for Maven in your program at the following coordinates:

```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>spark-tarantool-connector</artifactId>
  <version>0.1.1</version>
</dependency>
```

or for `sbt`:

```
libraryDependencies += "io.tarantool" %% "spark-tarantool-connector" % "0.1.1"
```

## Version Compatibility

| Connector | Scala   | Apache Spark | Tarantool Server |
| --------- | ------- | ------------ | ---------------- |
| 1.x.x     | 2.11.12 | 2.2, 2.4     | 1.10.9+,  2.4+   |
| 1.x.x     | 2.12.14 | 2.2, 2.4     | 1.10.9+,  2.4+   |

## Getting Started

### Configuration properties

| property-key                            | description                                          | default value   |
| --------------------------------------- | ---------------------------------------------------- | --------------- |
| tarantool.hosts                         | comma separated list of Tarantool hosts              | 127.0.0.1:3301  |
| tarantool.username                      | basic authentication user                            | guest           |
| tarantool.password                      | basic authentication password                        |                 |
| tarantool.connectTimeout                | server connect timeout, in milliseconds              | 1000            |
| tarantool.readTimeout                   | socket read timeout, in milliseconds                 | 1000            |
| tarantool.requestTimeout                | request completion timeout, in milliseconds          | 2000            |
| tarantool.cursorBatchSize               | default limit for prefetching tuples in RDD iterator | 1000            |

### Dataset API request options

| property-key                            | description                                    | default value   |
| --------------------------------------- | -----------------------------------------------| --------------- |
| tarantool.space                         | Tarantool space name                           |                 |
| tarantool.batchSize                     | limit of records to be read or written at once | 1000            |

#### Example

Using Scala:
```scala
    // 1. Set up the Spark session
    val spark = SparkSession.builder()
       .config("tarantool.hosts", "127.0.0.1:3301")
       .config("tarantool.user", "admin")
       .config("tarantool.password", "password")
       .getOrCreate()
    
    val sc = spark.sparkContext
    
    // 2. Load the whole space
    val rdd: Array[TarantoolTuple] = sc.tarantoolSpace("test_space").collect()

    // 3. Filter using conditions
    // This mapper will be used implicitly for tuple conversion
    val mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()
    
    val startTuple = new DefaultTarantoolTupleFactory(mapper).create(List(1).asJava)
    val cond: Conditions = Conditions
        .indexGreaterThan("id", List(1).asJava)
        .withLimit(2)
        .startAfter(startTuple)
    val tuples: Array[TarantoolTuple] = sc.tarantoolSpace("test_space", cond).collect()

    // 4. Load the whole space into a DataFrame
    val df = spark.read
      .format("org.apache.spark.sql.tarantool")
      .option("tarantool.space", "test_space")
      .load()
    
    // Space schema from Tarantool will be used for mapping the tuple fields
    val tupleIDs: Array[Int] = df.select("id").rdd.map(row => row.get(0)).collect()
```

or Java:
```java
    // 1. Set up the Spark context
    SparkConf conf = new SparkConf()
        .set("tarantool.hosts", "127.0.0.1:3301")
        .set("tarantool.user", "admin")
        .set("tarantool.password", "password");

    JavaSparkContext jsc = new JavaSparkContext(conf);

    // 2. Load all tuples from a space using custom tuple to POJO conversion
    List<Book> tuples = TarantoolSpark.contextFunctions(jsc)
        .tarantoolSpace("test_space", Conditions.any(), t -> {
            Book book = new Book();
            book.id = t.getInteger("id");
            book.name = t.getString("name");
            book.author = t.getString("author");
            book.year = t.getInteger("year");
            return book;
        }, Book.class).collect();
    
    // 3. Load all tuples from a space into a Dataset
    Dataset<Row> ds = spark().read()
        .format("org.apache.spark.sql.tarantool")
        .option("tarantool.space", "test_space")
        .load();

    ds.select("id").rdd().toJavaRDD().map(row -> row.get(0)).collect();
```

## Learn more

- [Tarantool](https://www.tarantool.io/)