package com.khaale.bigdatarampup.etl

import com.khaale.bigdatarampup.models.{BiddingBundle, BiddingSession}
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
object BiddingSessionizationProcessor {

  def process(input:DStream[BiddingBundle]) = {
    input
      .map(x => x.ipinyouId -> x)
      .updateStateByKey(SessionStateManager.updateFunc)
      .map{ case(key, session) => session }
  }
}

object SessionStateManager {

  def updateFunc(newBidBundles:Seq[BiddingBundle], sessionOpt:Option[BiddingSession]):Option[BiddingSession] = {

    //new session
    if (sessionOpt.isEmpty) {

      Some(BiddingSession(newBidBundles))
    }
    else {
      val oldSession = sessionOpt.get

      //update existing session
      if (newBidBundles.nonEmpty)
      {
        Some(oldSession.open(newBidBundles))
      }
      //check if we could close a session
      else {
        if (oldSession.isExpired) {
          //if session not closed yet - close it. If it's already closed - just remove it
          if (oldSession.status != BiddingSession.stateClosed) Some(oldSession.close()) else None
        }
        else Some(oldSession.open)
      }
    }
  }
}

