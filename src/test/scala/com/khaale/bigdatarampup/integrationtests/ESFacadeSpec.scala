package com.khaale.bigdatarampup.integrationtests

import java.util.UUID

import com.khaale.bigdatarampup.shared.cassandra.{CassandraFacade, CassandraSettings}
import com.khaale.bigdatarampup.shared.elasticsearch.{ESFacade, ESSettings}
import com.khaale.bigdatarampup.testing.fixtures.BiddingFixture
import com.khaale.bigdatarampup.testing.spark.SparkSpec
import org.apache.spark.Logging
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class ESFacadeSpec extends FeatureSpec with Matchers with SparkSpec with GivenWhenThen with Logging {

  val settings = ESSettings("mss_test")

  feature("Saving bidding bundle") {
    scenario("Save new bidding bundle") {

      Given("A bundle")
      val bundle = BiddingFixture.createBundle(ipinyouId = UUID.randomUUID().toString)
      val rdd = sc.parallelize(Seq(bundle))

      When("I save bundle to Cassandra")
      ESFacade.saveBiddingBundles(rdd, settings)
    }

    /*
    scenario("Save new bidding session") {

      Given("A session")
      val bundle = BiddingFixture.createSession(ipinyouId = UUID.randomUUID().toString)
      val rdd = sc.parallelize(Seq(bundle))

      When("I save session to Cassandra")
      CassandraFacade.saveBiddingSession(rdd, settings)
    }*/
  }

}
