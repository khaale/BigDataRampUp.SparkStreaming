package com.khaale.bigdatarampup.models

import com.khaale.bigdatarampup.shared.Clock

import scala.collection.mutable

/**
  * Created by Aleksander_Khanteev on 5/25/2016.
  */
case class BiddingSession(
                           ipinyouId:String,
                           tsStart:Long,
                           tsEnd:Long,
                           tsCurrent:Long,
                           status:Byte,
                           keywordCounters:Map[String,Long],
                           bidCounters:Map[DicLogType, Long],
                           state:DicState,
                           city:DicCity,
                           logType:DicLogType,
                           adExchange: DicAdExchange,
                           userAgent: UserAgent) {

  def open(bidBundles:Seq[BiddingBundle]): BiddingSession = {

    val lastBid = bidBundles.last
    val counters = BiddingSession.getCounters(bidBundles, keywordCounters, bidCounters)

    BiddingSession(ipinyouId, tsStart, lastBid.ts, Clock.getTime, BiddingSession.stateOpened, counters._1, counters._2, state, city, logType, adExchange, userAgent)
  }

  def open:BiddingSession = {
    BiddingSession(ipinyouId, tsStart, tsEnd, tsCurrent, BiddingSession.stateOpened, keywordCounters, bidCounters, state, city, logType, adExchange, userAgent)
  }

  def isExpired:Boolean = {

    val currentTime = Clock.getTime
    currentTime - tsCurrent >= BiddingSession.sessionTimeout
  }

  def close() = {
    BiddingSession(ipinyouId, tsStart, tsEnd, tsCurrent, BiddingSession.stateClosed, keywordCounters, bidCounters, state, city, logType, adExchange, userAgent)
  }
}

object BiddingSession {

  val sessionTimeout = 30 * 60 * 1000

  val maxBidsPerSession = 100

  val stateNew:Byte = 1
  val stateOpened:Byte = 2
  val stateClosed:Byte = 3

  private def mergeCounters[A](arr:Seq[A], map:Map[A,Long]):Map[A,Long] = {

    val mutableMap = mutable.HashMap(map.toArray:_*)

    arr.foreach(key => mutableMap.get(key) match {
      case Some(cnt) => mutableMap(key) = cnt + 1
      case None => mutableMap(key) = 1
    })

    mutableMap.toMap[A,Long]
  }

  private def getCounters(bids:Seq[BiddingBundle], currentKeywordCounters:Map[String,Long], currentBidCounters:Map[DicLogType, Long]) = {

    val newKeywords = bids.flatMap(b => b.extUserTags.keywords)
    val kwc = mergeCounters(newKeywords, currentKeywordCounters)

    val newBids = bids.map(b => b.extLogType)
    val bc = mergeCounters(newBids, currentBidCounters)

    kwc -> bc
  }

  def apply(bids:Seq[BiddingBundle]) = {

    val firstBid = bids.head
    val lastBid = bids.last

    val counters = getCounters(bids, Map.empty, Map.empty)

    new BiddingSession(
      firstBid.ipinyouId,
      firstBid.ts,
      lastBid.ts,
      Clock.getTime,
      BiddingSession.stateNew,
      counters._1,
      counters._2,
      firstBid.extState,
      firstBid.extCity,
      firstBid.extLogType,
      firstBid.extAdExchange,
      firstBid.extUserAgent
    )
  }
}
