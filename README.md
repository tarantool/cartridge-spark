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
  <version>0.3.0</version>
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
       .config("tarantool.username", "admin")
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

    // 5. Write a Dataset to a Tarantool space

    // Convert objects to Rows
    val rows = Seq(
      Book(1, null, "Don Quixote", "Miguel de Cervantes", 1605),
      Book(2, null, "The Great Gatsby", "F. Scott Fitzgerald", 1925),
      Book(2, null, "War and Peace", "Leo Tolstoy", 1869)
    ).map(obj => Row(obj.id, obj.bucketId, obj.bookName, obj.author, obj.year))

    // Extract an object schema using build-in Encoders
    val orderSchema = Encoders.product[Book].schema

    // Populate the Dataset
    val ds = spark.createDataFrame(rows, orderSchema)

    // Write to the space. Different modes are supported
    ds.write
      .format("org.apache.spark.sql.tarantool")
      .mode(SaveMode.Overwrite)
      .option("tarantool.space", "test_space")
      .save()
```

or Java:
```java
    // 1. Set up the Spark context
    SparkConf conf = new SparkConf()
        .set("tarantool.hosts", "127.0.0.1:3301")
        .set("tarantool.username", "admin")
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
    
    // 4. Write a Dataset to a Tarantool space
        
    // Create the schema first
    StructField[] structFields = new StructField[5];
    structFields[0] = new StructField("id", DataTypes.IntegerType, false, Metadata.empty());
    structFields[1] = new StructField("bucket_id", DataTypes.IntegerType, false, Metadata.empty());
    structFields[2] = new StructField("book_name", DataTypes.StringType, false, Metadata.empty());
    structFields[3] = new StructField("author", DataTypes.StringType, false, Metadata.empty());
    structFields[4] = new StructField("year", DataTypes.IntegerType, true, Metadata.empty());

    StructType schema = new StructType(structFields);

    // Populate the Dataset
    List<Row> data = new ArrayList<>(3);
    data.add(RowFactory.create(1, null, "Don Quixote", "Miguel de Cervantes", 1605));
    data.add(RowFactory.create(2, null, "The Great Gatsby", "F. Scott Fitzgerald", 1925));
    data.add(RowFactory.create(3, null, "War and Peace", "Leo Tolstoy", 1869));

    Dataset<Row> ds = sqlContext.createDataFrame(data, schema);

    // Write to the space. Different modes are supported
    ds.write()
        .format("org.apache.spark.sql.tarantool")
        .mode(SaveMode.Overwrite)
        .option("tarantool.space", "test_space")
        .save();
```

## Supported DataSet write modes

Consult with the following table about what will happen when a DataSet is written with different modes.
In all modes it is supposed that all the spaces used in an operation exist. An error will be produced otherwise. 

| Mode          | How it works                                                                                       |
|---------------|----------------------------------------------------------------------------------------------------|
| Append        | If a record with the given primary key exists, it will be replaced, and inserted otherwise.        |
| Overwrite     | The space will be truncated before writing the DataSet, and then the records will be inserted.     |
| ErrorIfExists | If the space is not empty, an error will be produced; otherwise, the records will be inserted.     |
| Ignore        | If the space is not empty, no records will be inserted an no errors will be produced.              |

## Learn more

- [Tarantool](https://www.tarantool.io/)
