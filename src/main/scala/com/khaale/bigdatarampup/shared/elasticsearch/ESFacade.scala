package com.khaale.bigdatarampup.shared.elasticsearch

import java.sql.Date

import com.khaale.bigdatarampup.models.{UserAgent, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, LocalDate}

/**
  * Created by Aleksander_Khanteev on 5/29/2016.
  */
object ESFacade {

  val format = DateTimeFormat.forPattern("yyyy.MM.dd")

  def saveBiddingBundles(input:RDD[BiddingBundle], settings:ESSettings): Unit = {

    val sqlc = SQLContext.getOrCreate(input.sparkContext)
    import sqlc.implicits._
    import org.elasticsearch.spark.sql._

    //It's not too optimal solution (we need to iterate 1 + NUM UNIQUE DATES times)
    //But, normally, cases with more than one date may occur only once a day (for the last day's batch)
    val uniqueDates = input.map(_.getLocalDate).distinct().collect()
    for (date <- uniqueDates) {
      input
        .filter(x => x.getLocalDate.equals(date))
        .map(x => ESBiddingBundle(x))
        .toDF()
        .saveToEs(getResourceName(settings.indexPrefix, date, "bidding_bundle"), Map(ConfigurationOptions.ES_MAPPING_ID -> "bidId"))
    }
  }

  private def getResourceName(indexPrefix:String, date:LocalDate, typeName:String): String = {
    s"$indexPrefix-${date.toString(format)}/$typeName"
  }
}

case class ESSettings(indexPrefix:String)


class ESBiddingBundle(
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
                     val payingPrice:Double,
                     val creativeId:String,
                     val biddingPrice:Double,
                     val advertiserId:Int,
                     val userTags:Long,
                     val streamId:Int,

                     val extState:DicState,
                     val extCity:DicCity,
                     val extLogType: DicLogType,
                     val extAdExchange: DicAdExchange,
                     val extUserTags:DicTags,
                     val extUserAgent:UserAgent,
                     val keywords: String,
                     val location: ESLocation
                   ) extends Product
  //For Spark it has to be Serializable
  with Serializable {

  def getDateTime = new DateTime(ts, DateTimeZone.UTC)
  def getLocalDate = getDateTime.toLocalDate

  def canEqual(that: Any) = that.isInstanceOf[BiddingBundle]

  def productArity = 29 // number of columns

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

    case 28 => keywords
    case 29 => location
  }
}

object ESBiddingBundle {

  def apply(src:BiddingBundle): ESBiddingBundle ={

    new ESBiddingBundle(
      src.bidId,
      src.ts,
      src.ipinyouId,
      src.userAgent,
      src.ip,
      src.region,
      src.city,
      src.adExchange,
      src.domain,
      src.url,
      src.anonymousUrl,
      src.adSlotId,
      src.adSlotWidth,
      src.adSlotHeight,
      src.adSlotVisibility,
      src.adSlotFormat,
      src.payingPrice.toDouble,
      src.creativeId,
      src.biddingPrice.toDouble,
      src.advertiserId,
      src.userTags,
      src.streamId,

      src.extState,
      src.extCity,
      src.extLogType,
      src.extAdExchange,
      src.extUserTags,
      src.extUserAgent,

      src.extUserTags.keywords.mkString(","),
      ESLocation(src.extCity.latitude, src.extCity.longitude)
    )
  }
}

case class ESLocation(lat:Double, lon:Double)
