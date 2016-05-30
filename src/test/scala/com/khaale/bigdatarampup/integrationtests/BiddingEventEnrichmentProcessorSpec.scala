package com.khaale.bigdatarampup.integrationtests

import com.khaale.bigdatarampup.etl.{BiddingEventEnrichmentProcessor, DictionaryLoader}
import com.khaale.bigdatarampup.models._
import com.khaale.bigdatarampup.testing.fixtures.BiddingFixture
import com.khaale.bigdatarampup.testing.spark.SparkStreamingSpec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class BiddingEventEnrichmentProcessorSpec extends FeatureSpec with Matchers with SparkStreamingSpec with Eventually with GivenWhenThen with MockFactory {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5000, Millis)))

  feature("Message enrichment") {
    scenario("Creating a bundle with extended information") {

      Given("streaming context is initalized")
      val input = BiddingFixture.createEvent()

      val inputQueue = mutable.Queue[RDD[BiddingEvent]]()
      val results = mutable.ListBuffer.empty[BiddingBundle]

      BiddingEventEnrichmentProcessor
        .process(sc, new MockableDictionaryLoader(sc))(ssc.queueStream(inputQueue, oneAtATime = true))
        .foreachRDD(rdd => results ++= rdd.collect())

      ssc.start()

      When("one message is queued")
      inputQueue.enqueue(sc.parallelize(Seq(input)))

      Then("message count after the first batch")
      advanceClockOneBatch()
      eventually {
        results.length should equal(1)
      }
    }
  }
}

class MockableDictionaryLoader(sc:SparkContext) extends DictionaryLoader(sc, null) {
  import sqlc.implicits._
  def makeRdd[A:ClassTag](item:A):RDD[A] = sc.parallelize(Seq(item))

  override def loadStates() = makeRdd(DicState(79, "New York", 19651127, 1350286)).toDS()
  override def loadCities() = makeRdd(DicCity(1, "New York", 79, 8491079, 783.8f, 10430, 40.6643f, -73.9385f)).toDS()
  override def loadLogTypes() = makeRdd(DicLogType(0, "unknown")).toDS()
  override def loadAdExchanges() = makeRdd(DicAdExchange(3, "Baidu", "Baidu")).toDS()
  override def loadTags() = makeRdd(DicTags(282162995497L, "cars,auto", "", "", "", "")).toDS()
}