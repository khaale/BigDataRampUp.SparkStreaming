package com.khaale.bigdatarampup.testing.spark

import org.apache.spark.sql.SQLContext
import org.scalatest.Suite

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  def sqlc: SQLContext = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = SQLContext.getOrCreate(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }

}
