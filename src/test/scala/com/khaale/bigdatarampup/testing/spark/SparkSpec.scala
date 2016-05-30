package com.khaale.bigdatarampup.testing.spark

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  private var _sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.port","14041")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cleaner.ttl", "3600")
      .set("es.index.auto.create", "true")
      .setAppName(this.getClass.getSimpleName)

    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    _sc = new SparkContext(conf)
  }

  def sparkConfig: Map[String, String] = Map.empty

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }

  def sc: SparkContext = _sc

}



