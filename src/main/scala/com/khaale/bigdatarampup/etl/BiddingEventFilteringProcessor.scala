package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models.BiddingEvent
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
object BiddingEventFilteringProcessor {

  def process(input:DStream[BiddingEvent]): DStream[BiddingEvent] = {

    input.filter(x => x.hasIpinyouId && x.streamId != 0)
  }
}
