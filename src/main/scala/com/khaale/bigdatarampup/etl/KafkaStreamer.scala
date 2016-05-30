package com.khaale.bigdatarampup.etl

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Aleksander_Khanteev on 5/25/2016.
  */
object KafkaStreamer {

  def getStream(ssc: StreamingContext, opts:KafkaStreamerSettings):DStream[(String,String)] = {

    //configuring stream
    val kafkaParams = Map("metadata.broker.list" -> opts.broker)

    // Define which topics to read from
    val topics = Set(opts.topic)

    // Create the direct stream with the Kafka parameters and topics
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  }
}

case class KafkaStreamerSettings(broker:String, topic:String)
