package io.tarantool.spark.connector.integration;

import java.io.Serializable;

/**
 * @author Alexey Kuzin
 */
public class Book implements Serializable {
    public Integer id;

    public String name;

    public String author;

    public Integer year;
}
