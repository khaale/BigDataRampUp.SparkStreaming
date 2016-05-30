package com.khaale.bigdatarampup.integrationtests

import com.khaale.bigdatarampup.etl.BiddingEventParsingProcessor
import com.khaale.bigdatarampup.models.BiddingEvent
import com.khaale.bigdatarampup.testing.spark.SparkStreamingSpec
import org.apache.spark.rdd.RDD
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class BiddingEventParsingProcessorSpec extends FeatureSpec with Matchers with SparkStreamingSpec with Eventually with GivenWhenThen {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5000, Millis)))

  feature("Raw message parsing") {
    scenario("Parsing single message") {

      Given("streaming context is initalized")
      val inputMessage = "f65fce1aab865d8cdea808fbab2f624b\t20130607233212952\tVhkR1aq4DoCIQOE\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t182.18.2.*\t1\t1\t3\t31xSTvprdN1RFt\t4bca76c166cc4c5091839c5c4c5e8e6b\tnull\tEnt_F_Upright\t300\t250\t0\t0\t50\t44966cc8da1ed40c95d59e863c8c75f0\t300\t3386\t282162995497\t0"

      val input = mutable.Queue[RDD[(String,String)]]()
      val results = mutable.ListBuffer.empty[BiddingEvent]

      BiddingEventParsingProcessor
        .process(ssc.queueStream(input, oneAtATime = true))
        .foreachRDD(rdd => results ++= rdd.collect())

      ssc.start()

      When("one message is queued")
      input.enqueue(sc.parallelize(Seq(("key1",inputMessage))))

      Then("message count after the first batch")
      advanceClockOneBatch()
      eventually {
        results.length should equal(1)
      }
    }
  }

}