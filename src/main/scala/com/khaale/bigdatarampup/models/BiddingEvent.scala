package com.khaale.bigdatarampup.models

import org.apache.spark.sql.types.Decimal
import org.joda.time.format.DateTimeFormat

/**
  * Created by Aleksander_Khanteev on 5/24/2016.
  */
case class BiddingEvent(
                           bidId:String,
                           ts:Long,
                           ipinyouId:String,
                           userAgent:String,
                           ip:String,
                           region:Int,
                           city:Int,
                           adExchange:Byte,
                           domain:String,
                           url:String,
                           anonymousUrl:String,
                           adSlotId:String,
                           adSlotWidth:Int,
                           adSlotHeight:Int,
                           adSlotVisibility:Int,
                           adSlotFormat:Int,
                           payingPrice:Decimal,
                           creativeId:String,
                           biddingPrice:Decimal,
                           advertiserId:Int,
                           userTags:Long,
                           streamId:Int
                         ) {

  def hasIpinyouId = ipinyouId != null && !ipinyouId.isEmpty && !ipinyouId.equals("null")
}


object BiddingEvent {

  private val formatter = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS").withZoneUTC()

  def apply(message:String) : BiddingEvent = {

    val fields = message.split("\t")
    val ts = fields(1) match {
      case x if x.startsWith("201") =>
        val date = formatter.parseDateTime(x)
        date.getMillis
      case x => x.toLong
    }

    new BiddingEvent(
      fields(0),
      ts,
      fields(2),
      fields(3),
      fields(4),
      fields(5).toInt,
      fields(6).toInt,
      fields(7).toByte,
      fields(8),
      fields(9),
      fields(10),
      fields(11),
      fields(12).toInt,
      fields(13).toInt,
      fields(14).toInt,
      fields(15).toInt,
      Decimal(fields(16)),
      fields(17),
      Decimal(fields(18)),
      fields(19).toInt,
      fields(20).toLong,
      fields(21).toInt
    )
  }
}
