package com.khaale.bigdatarampup.apps

import java.nio.file.Files

import com.khaale.bigdatarampup.etl._
import com.khaale.bigdatarampup.models.BiddingSession
import com.khaale.bigdatarampup.shared.{AppSettingsProvider, SparkSettings}
import com.khaale.bigdatarampup.shared.cassandra.CassandraFacade
import com.khaale.bigdatarampup.shared.elasticsearch.{ESFacade, ESSettings}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}


/**
  * Created by Aleksander_Khanteev on 5/18/2016.
  */
object StreamingApp extends App with Logging  {

  //set up configuration
  val settingsProvider = (if (args.length > 0) args(0) else "cfg/app.dev.conf") match {
    case confPath if confPath != null && !confPath.isEmpty => new AppSettingsProvider(confPath)
    case _ => throw new IllegalArgumentException("Configuration path must be specified as a first argument!")
  }
  val isTest = settingsProvider.isTestRun
  val kafkaSettingsOpt = settingsProvider.getKafkaSettings
  val sparkSettings = settingsProvider.getSparkSettings match {
    case Some(x) => x
    case None => SparkSettings(checkpointDir)
  }

  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    .set("spark.cassandra.connection.host", settingsProvider.getCassandraSettings.get.host)
    .set("spark.cleaner.ttl", "3600")
    .set("es.index.auto.create", "true")

  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.checkpoint(checkpointDir)
  val sc = ssc.sparkContext

  run()

  def run() {

    //configuring pipeline
    val inputStream = KafkaStreamer.getStream(ssc, kafkaSettingsOpt.get)
    val biddingEvents = BiddingEventParsingProcessor.process(inputStream)
    val filteredBiddingEvents = BiddingEventFilteringProcessor.process(biddingEvents)

    val dictionaryLoader = new DictionaryLoader(sc, settingsProvider.dictionarySettings)
    val biddingBundles = BiddingEventEnrichmentProcessor.process(sc, dictionaryLoader)(filteredBiddingEvents)

    val savedBundles = biddingBundles.transform(rdd => {
      rdd.cache()
      ESFacade.saveBiddingBundles(rdd, ESSettings("mss"))
      CassandraFacade.saveBiddingBundles(rdd, settingsProvider.getCassandraSettings.get)
      rdd
    })
    savedBundles.glom()

    val sessions = BiddingSessionizationProcessor.process(savedBundles)

    val savedSessions = sessions.transform(rdd => {
      rdd.cache()
      CassandraFacade.saveBiddingSession(rdd.filter(r => r.status == BiddingSession.stateClosed), settingsProvider.getCassandraSettings.get)
      rdd
    })

    savedSessions.foreachRDD(rdd => println(s"Processed ${rdd.count()} events!"))

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }

  def checkpointDir: String = Files.createTempDirectory(this.getClass.getSimpleName).toUri.toString

}





