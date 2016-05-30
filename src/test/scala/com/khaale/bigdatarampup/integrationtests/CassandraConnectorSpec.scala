package com.khaale.bigdatarampup.integrationtests

import com.datastax.spark.connector.cql.CassandraConnector
import com.khaale.bigdatarampup.testing.spark.SparkSpec
import org.apache.spark.Logging
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

/**
  * Just playing with Cassandra
  */
class CassandraConnectorSpec extends FeatureSpec with Matchers with SparkSpec with GivenWhenThen with Logging {


  feature("Playing with CassandraConnector") {
    scenario("Basic read write") {

      Given("test keyspace and table")
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")
        session.execute("TRUNCATE test.key_value")
        session.execute("INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
        session.execute("INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
        session.execute("INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")
      }

      import com.datastax.spark.connector._

      // Read table test.kv and print its contents:
      val rdd = sc.cassandraTable("test", "key_value").select("key", "value")
      rdd.collect().foreach(row => println(s"Existing Data: $row"))

      // Write two new rows to the test.kv table:
      val col = sc.parallelize(Seq((4, "fourth row"), (5, "fifth row")))
      col.saveToCassandra("test", "key_value", SomeColumns("key", "value"))

      // Assert the two new rows were stored in test.kv table:
      assert(col.collect().length == 2)

      col.collect().foreach(row => println(s"New Data: $row"))
      println(s"Work completed, stopping the Spark context.")
    }

    scenario("Write case class") {

      Given("test keyspace and table")
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")
        session.execute("TRUNCATE test.key_value")
      }
      import com.datastax.spark.connector._

      // Write two new rows to the test.kv table:
      val col = sc.parallelize(Seq(KV(4, "fourth row"), KV(5, "fifth row")))
      col.saveToCassandra("test", "key_value", SomeColumns("key", "value"))

      // Assert the two new rows were stored in test.kv table:
      assert(col.collect().length == 2)

      col.collect().foreach(row => println(s"New Data: $row"))
      println(s"Work completed, stopping the Spark context.")
    }

    scenario("Write case class with complex names") {

      Given("test keyspace and table")
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TABLE IF NOT EXISTS test.complex_name (my_key INT PRIMARY KEY, my_value VARCHAR)")
        session.execute("TRUNCATE test.complex_name")
      }
      import com.datastax.spark.connector._

      // Write two new rows to the test.kv table:
      val col = sc.parallelize(Seq(ComplexNames(4, "fourth row"), ComplexNames(5, "fifth row")))
      col.saveToCassandra("test", "complex_name")

      // Assert the two new rows were stored in test.kv table:
      assert(col.collect().length == 2)

      col.collect().foreach(row => println(s"New Data: $row"))
      println(s"Work completed, stopping the Spark context.")
    }

    scenario("Write case class with nested UDT") {

      Given("test keyspace and table")
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
        session.execute("CREATE TYPE IF NOT EXISTS test.my_udt (my_key int, my_value text)")
        session.execute("CREATE TABLE IF NOT EXISTS test.nested_udt (my_key INT PRIMARY KEY, my_value VARCHAR, nested FROZEN<test.my_udt>)")
        session.execute("TRUNCATE test.nested_udt")
      }
      import com.datastax.spark.connector._

      // Write two new rows to the test.kv table:
      val col = sc.parallelize(Seq(NestedUDT(4, "fourth row", ComplexNames(1,"123")), NestedUDT(5, "fifth row", ComplexNames(2,"asdfasdf"))))
      col.saveToCassandra("test", "nested_udt")

      // Assert the two new rows were stored in test.kv table:
      assert(col.collect().length == 2)

      col.collect().foreach(row => println(s"New Data: $row"))
      println(s"Work completed, stopping the Spark context.")
    }

  }

}

case class KV(key:Int, value:String)
case class ComplexNames(myKey:Int, myValue:String)
case class NestedUDT(myKey:Int, myValue:String, nested:ComplexNames)



