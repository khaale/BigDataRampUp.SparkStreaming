package com.khaale.bigdatarampup.shared.cassandra

import com.khaale.bigdatarampup.models.{BiddingBundle, BiddingSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.Decimal
import com.datastax.spark.connector.types._

import scala.reflect.runtime.universe._
/**
  * Created by Aleksander_Khanteev on 5/26/2016.
  */
object CassandraFacade {

  import com.datastax.spark.connector._

  object BigDecimalToDecimalConverter extends TypeConverter[Decimal] {
    def targetTypeTag = typeTag[Decimal]
    def convertPF = { case x: java.math.BigDecimal => Decimal(x) }
  }

  object DecimalToBigDecimalConverter extends TypeConverter[java.math.BigDecimal] {
    def targetTypeTag = typeTag[java.math.BigDecimal]

    def convertPF = { case x: Decimal => x.toBigDecimal.bigDecimal }
  }

  TypeConverter.registerConverter(BigDecimalToDecimalConverter)
  TypeConverter.registerConverter(DecimalToBigDecimalConverter)

  def saveBiddingBundles(input:RDD[BiddingBundle], settings: CassandraSettings):Unit = {

    input.saveToCassandra(settings.keyspace, "bidding_bundle")
  }

  def saveBiddingSession(input:RDD[BiddingSession], settings: CassandraSettings):Unit = {

    input.saveToCassandra(settings.keyspace, "bidding_session")
  }
}

case class CassandraSettings(host:String, keyspace:String)



