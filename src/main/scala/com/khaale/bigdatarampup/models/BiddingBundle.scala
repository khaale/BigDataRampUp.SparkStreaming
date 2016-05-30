package com.khaale.bigdatarampup.models

import org.apache.spark.sql.types.Decimal
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
class BiddingBundle(
                    val bidId:String,
                    val ts:Long,
                    val ipinyouId:String,
                    val userAgent:String,
                    val ip:String,
                    val region:Int,
                    val city:Int,
                    val adExchange:Byte,
                    val domain:String,
                    val url:String,
                    val anonymousUrl:String,
                    val adSlotId:String,
                    val adSlotWidth:Int,
                    val adSlotHeight:Int,
                    val adSlotVisibility:Int,
                    val adSlotFormat:Int,
                    val payingPrice:Decimal,
                    val creativeId:String,
                    val biddingPrice:Decimal,
                    val advertiserId:Int,
                    val userTags:Long,
                    val streamId:Int,

                    val extState:DicState,
                    val extCity:DicCity,
                    val extLogType: DicLogType,
                    val extAdExchange: DicAdExchange,
                    val extUserTags:DicTags,
                    val extUserAgent:UserAgent
) extends Product
  //For Spark it has to be Serializable
  with Serializable {

  def getDateTime = new DateTime(ts, DateTimeZone.UTC)
  def getLocalDate = getDateTime.toLocalDate

  def canEqual(that: Any) = that.isInstanceOf[BiddingBundle]

  def productArity = 28 // number of columns

  def productElement(idx: Int) = idx match {
    case 0 => bidId
    case 1 => ts
    case 2 => ipinyouId
    case 3 => userAgent
    case 4 => ip
    case 5 => region
    case 6 => city
    case 7 => adExchange
    case 8 => domain
    case 9 => url
    case 10 => anonymousUrl
    case 11 => adSlotId
    case 12 => adSlotWidth
    case 13 => adSlotHeight
    case 14 => adSlotVisibility
    case 15 => adSlotFormat
    case 16 => payingPrice
    case 17 => creativeId
    case 18 => biddingPrice
    case 19 => advertiserId
    case 20 => userTags
    case 21 => streamId

    case 22 => extState
    case 23 => extCity
    case 24 => extLogType
    case 25 => extAdExchange
    case 26 => extUserTags
    case 27 => extUserAgent
  }
}

object BiddingBundle {

  def apply(bidingEvent:BiddingEvent,
            extState:DicState,
            extCity:DicCity,
            extLogType: DicLogType,
            extAdExchange: DicAdExchange,
            extUserTags:DicTags,
            extUserAgent:UserAgent): BiddingBundle ={

    new BiddingBundle(
      bidingEvent.bidId,
      bidingEvent.ts,
      bidingEvent.ipinyouId,
      bidingEvent.userAgent,
      bidingEvent.ip,
      bidingEvent.region,
      bidingEvent.city,
      bidingEvent.adExchange,
      bidingEvent.domain,
      bidingEvent.url,
      bidingEvent.anonymousUrl,
      bidingEvent.adSlotId,
      bidingEvent.adSlotWidth,
      bidingEvent.adSlotHeight,
      bidingEvent.adSlotVisibility,
      bidingEvent.adSlotFormat,
      bidingEvent.payingPrice,
      bidingEvent.creativeId,
      bidingEvent.biddingPrice,
      bidingEvent.advertiserId,
      bidingEvent.userTags,
      bidingEvent.streamId,

      extState,
      extCity,
      extLogType,
      extAdExchange,
      extUserTags,
      extUserAgent
    )
  }
}

